---------------- MODULE HawDBVersionHistoryBudget ----------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS Keys, Budget, MaxEpoch, SkipAdmission, PrunePinned,
          SkipRefund, ReserveDatabase
D == "database"
Ids == Keys \union {D}
Weights == [k \in Ids |-> IF k = "b" THEN 2 ELSE 1]
WriteSets == ((SUBSET Keys) \ {{}}) \union {{D}}
RECURSIVE Sum(_)
Sum(s) == IF s = {} THEN 0 ELSE LET k == CHOOSE x \in s: TRUE IN Weights[k] + Sum(s \ {k})
VARIABLES stamps, charge, pin, history, refused, legacyAtCapacity, pressureReclaimed
vars == <<stamps, charge, pin, history, refused, legacyAtCapacity, pressureReclaimed>>
Epoch == Len(history)
Present(s) == {k \in Ids: s[k] > 0}
Account(s) == IF ReserveDatabase THEN 1 + Sum(Present(s) \ {D}) ELSE Sum(Present(s))
Actual(s) == Sum(Present(s))
Broad(w) == D \in w
Overlap(a,b) == a \intersect b # {} \/ Broad(a) \/ Broad(b)
ReferenceValid(w,e) == \A i \in (e+1)..Epoch: ~Overlap(w,history[i])
IndexValid(w,e) == /\ \A k \in w: stamps[k] <= e
                   /\ stamps[D] <= e
                   /\ (Broad(w) => Epoch <= e)
SafePruned == [k \in Ids |-> IF PrunePinned \/ pin = -1 \/ stamps[k] < pin THEN 0 ELSE stamps[k]]
Refund(s) == IF SkipRefund THEN charge ELSE charge - (Account(stamps) - Account(s))
Published(s,w) == [k \in Ids |-> IF k \in w THEN Epoch+1 ELSE s[k]]
Growth(s,w) == Account(Published(s,w)) - Account(s)
Candidate(w) == IF charge + Growth(stamps,w) > Budget THEN SafePruned ELSE stamps
CandidateCharge(w) == IF Candidate(w) = stamps THEN charge ELSE Refund(Candidate(w))
Init == /\ stamps = [k \in Ids |-> 0]
        /\ charge = IF ReserveDatabase THEN 1 ELSE 0
        /\ pin = -1 /\ history = <<>>
        /\ refused = FALSE /\ legacyAtCapacity = FALSE /\ pressureReclaimed = FALSE
Capture == /\ pin = -1 /\ pin' = Epoch
           /\ UNCHANGED <<stamps,charge,history,refused,legacyAtCapacity,pressureReclaimed>>
Drop == /\ pin >= 0 /\ pin' = -1
        /\ UNCHANGED <<stamps,charge,history,refused,legacyAtCapacity,pressureReclaimed>>
Commit(w,e) ==
    /\ Epoch < MaxEpoch
    /\ IndexValid(w,e)
    /\ LET s == Candidate(w)
           c == CandidateCharge(w)
           next == c + Growth(s,w)
       IN /\ SkipAdmission \/ next <= Budget
          /\ stamps' = Published(s,w) /\ charge' = next
          /\ history' = Append(history,w)
          /\ pressureReclaimed' = (pressureReclaimed \/ s # stamps)
    /\ UNCHANGED <<pin,refused,legacyAtCapacity>>
Refuse(w,e) ==
    /\ IndexValid(w,e)
    /\ CandidateCharge(w) + Growth(Candidate(w),w) > Budget
    /\ stamps' = Candidate(w) /\ charge' = CandidateCharge(w)
    /\ refused' = TRUE
    /\ UNCHANGED <<pin,history,legacyAtCapacity,pressureReclaimed>>
(* Legacy commits do not perform history admission after WAL: D must be prepaid. *)
Legacy ==
    /\ Epoch < MaxEpoch
    /\ stamps' = Published(stamps,{D})
    /\ charge' = charge + Growth(stamps,{D})
    /\ history' = Append(history,{D})
    /\ legacyAtCapacity' = (legacyAtCapacity \/ charge = Budget)
    /\ UNCHANGED <<pin,refused,pressureReclaimed>>
Prune == /\ SafePruned # stamps
         /\ stamps' = SafePruned /\ charge' = Refund(SafePruned)
         /\ UNCHANGED <<pin,history,refused,legacyAtCapacity,pressureReclaimed>>
Baseline == [k \in Ids |-> IF k = D THEN Epoch ELSE 0]
Compact == /\ Baseline # stamps
           /\ PrunePinned \/ pin = -1 \/ pin >= Epoch
           /\ stamps' = Baseline /\ charge' = Refund(Baseline)
           /\ UNCHANGED <<pin,history,refused,legacyAtCapacity,pressureReclaimed>>
Next == Capture \/ Drop \/ Legacy \/ Prune \/ Compact \/ (\E w \in WriteSets, e \in ({Epoch,pin} \ {-1}): Commit(w,e) \/ Refuse(w,e))
TypeInvariant == /\ stamps \in [Ids -> 0..MaxEpoch]
                 /\ charge \in Nat /\ pin \in (-1)..Epoch
                 /\ history \in Seq(WriteSets) /\ Epoch <= MaxEpoch
                 /\ {refused,legacyAtCapacity,pressureReclaimed} \subseteq BOOLEAN
ExactAccounting == charge = Account(stamps)
BoundedCurrentPayload == Actual(stamps) <= charge /\ charge <= Budget
ValidationMatchesHistory == \A e \in (IF pin = -1 THEN {Epoch} ELSE pin..Epoch):
                               \A w \in WriteSets: IndexValid(w,e) = ReferenceValid(w,e)
NoRefusalWitness == ~refused
NoLegacyAtCapacityWitness == ~legacyAtCapacity
NoPressureReclaimWitness == ~pressureReclaimed
NoTipCompactionWitness == ~(Epoch > 0 /\ pin = Epoch /\ stamps = Baseline /\ history[Epoch] \subseteq Keys)
=============================================================================
