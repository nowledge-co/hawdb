-------------------- MODULE HawDBHeldGenerationSpool --------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS ReopenLabel, ForgeMissingOwner, SharedOffset, CloseBeforeReaders,
          RefundBeforeClose, FreshAccount, HoldAfterBuild
VARIABLE s
vars == <<s>>
Init == s = [phase |-> "writing", label |-> 1, owner |-> TRUE, held |-> TRUE,
             funded |-> TRUE, account |-> 1, writable |-> TRUE,
             readers |-> {}, used |-> {}, pos |-> [i \in {0,1} |-> 0],
             file |-> [i \in {0,1} |-> 0], global |-> 0,
             wrongProducer |-> FALSE, wrongPosition |-> FALSE,
             forged |-> FALSE, complete |-> FALSE]
ReplaceLabel == /\ s.phase \in {"writing", "sealed"} /\ s.label = 1
                /\ s' = [s EXCEPT !.label = 2]
Seal == /\ s.phase = "writing" /\ s.owner
        /\ s' = [s EXCEPT !.phase = "sealed", !.writable = FALSE,
                           !.account = IF FreshAccount THEN 2 ELSE @]
Open == /\ s.phase = "sealed" /\ (s.owner \/ s.readers # {} \/ ForgeMissingOwner)
        /\ \E i \in {0,1} \ s.used :
             s' = [s EXCEPT !.readers = @ \cup {i}, !.used = @ \cup {i},
                   !.file[i] = IF ReopenLabel \/ (~s.owner /\ s.readers = {}) THEN s.label ELSE 1,
                   !.forged = @ \/ (~s.owner /\ s.readers = {})]
Read == /\ s.phase = "sealed" /\ s.held
        /\ \E i \in s.readers :
             /\ s.pos[i] < 2
             /\ s' = [s EXCEPT !.pos[i] = @ + 1,
                   !.global = IF @ < 4 THEN @ + 1 ELSE @,
                   !.wrongProducer = @ \/ s.file[i] # 1,
                   !.wrongPosition = @ \/ (SharedOffset /\ s.global # s.pos[i]),
                   !.complete = @ \/ s.pos[i] = 1]
CloseReader == /\ \E i \in s.readers :
                    LET remaining == s.readers \ {i} IN
                      s' = [s EXCEPT !.readers = remaining,
                            !.held = s.owner \/ remaining # {},
                            !.funded = s.owner \/ remaining # {}]
CloseOwner == /\ s.owner /\ s.phase \in {"writing", "sealed"}
              /\ s' = [s EXCEPT !.owner = FALSE,
                    !.held = s.readers # {} /\ ~CloseBeforeReaders,
                    !.funded = s.readers # {} /\ ~RefundBeforeClose]
Build == /\ s.phase = "sealed" /\ s.owner /\ s.complete /\ s.readers = {}
         /\ s' = [s EXCEPT !.phase = "built", !.owner = FALSE,
                            !.held = HoldAfterBuild, !.funded = HoldAfterBuild]
Publish == /\ s.phase = "built" /\ s' = [s EXCEPT !.phase = "published"]
Next == ReplaceLabel \/ Seal \/ Open \/ Read \/ CloseReader \/ CloseOwner \/ Build \/ Publish
TypeOK == /\ s.phase \in {"writing", "sealed", "built", "published"}
          /\ s.label \in {1,2} /\ s.account \in {1,2}
          /\ s.readers \subseteq s.used /\ s.used \subseteq {0,1}
          /\ s.pos \in [{0,1} -> 0..2] /\ s.file \in [{0,1} -> 0..2]
          /\ s.global \in 0..4
          /\ \A f \in {"owner", "held", "funded", "writable", "wrongProducer",
               "wrongPosition", "forged", "complete"} : s[f] \in BOOLEAN
ActualProducer == ~s.wrongProducer
OwnerRequired == ~s.forged
IndependentPositions == ~s.wrongPosition
ReadersKeepNativeFile == s.readers # {} => s.held
NativeCloseBeforeRefund == s.held => s.funded
OriginalMemory == s.account = 1
ReadersOnlyAfterSeal == s.readers # {} => ~s.writable
PreparedReleasesInput == s.phase \in {"built", "published"} => ~s.held /\ ~s.owner /\ s.readers = {}
Spec == Init /\ [][Next]_vars
=============================================================================
