---------------- MODULE HawDBSearchMutationPublication ----------------
EXTENDS Integers, FiniteSets

CONSTANTS Readers, GlobalIdMask, SkipCAS, PublishEarly, ReclaimPinned,
          OrphanCompaction

(**************************************************************************)
(* Finite publication protocol: two logical documents, repeated replacement
   of a, deletion, complete-closure compaction, and a competing deletion of b.
   Generation identities are unique. Payload values are independent of the
   containing segment, so compaction must preserve the logical document set.
   Atomic durable-file completion and selector replacement abstract fsync and
   checksums; this is not a model of byte-level I/O or all query algorithms. *)
(**************************************************************************)

Gens == 0..5
Version(id, segment, value) == [id |-> id, segment |-> segment, value |-> value]
A0 == Version("a", 0, 0)
B0 == Version("b", 0, 0)
A1 == Version("a", 1, 1)
A2 == Version("a", 2, 2)
Run(gen, id, target) == [run |-> gen, id |-> id, target |-> target]
R1 == Run(1, "a", 0)
R2 == Run(2, "a", 1)
R3 == Run(3, "a", 2)
R5 == Run(5, "b", 0)

Content(g) == CASE g = 0 -> {A0, B0}
               [] g = 1 -> {A0, B0, A1}
               [] g \in {2, 3} -> {A0, B0, A1, A2}
               [] g = 4 -> {Version("a", 4, 2), Version("b", 4, 0)}
               [] g = 5 -> {A0, B0}
Runs(g) == CASE g = 0 -> {}
            [] g = 1 -> {R1}
            [] g = 2 -> {R1, R2}
            [] g = 3 -> {R1, R2, R3}
            [] g = 4 -> IF OrphanCompaction THEN {R1, R2} ELSE {}
            [] g = 5 -> {R5}
Parent(g) == CASE g \in {1, 5} -> 0
              [] g = 2 -> 1
              [] g \in {3, 4} -> 2
Expected(g) == CASE g = 0 -> {<<"a", 0>>, <<"b", 0>>}
                [] g = 1 -> {<<"a", 1>>, <<"b", 0>>}
                [] g \in {2, 4} -> {<<"a", 2>>, <<"b", 0>>}
                [] g = 3 -> {<<"b", 0>>}
                [] g = 5 -> {<<"a", 0>>}

Visible(g) == {v \in Content(g): ~\E r \in Runs(g):
                 r.id = v.id /\ (GlobalIdMask \/ r.target = v.segment)}
Logical(g) == {<<v.id, v.value>>: v \in Visible(g)}
Closure(g) == {<<"content", v.segment>>: v \in Content(g)}
              \cup {<<"run", r.run>>: r \in Runs(g)}
Files == UNION {Closure(g): g \in Gens}
BoundTargets(g) == \A r \in Runs(g):
                    \E v \in Content(g): v.id = r.id /\ v.segment = r.target

VARIABLES active, candidate, phase, durable, pins, published, stalePublished
vars == <<active, candidate, phase, durable, pins, published, stalePublished>>
Selected == ({active} \cup {pins[r]: r \in Readers}) \ {-1}
SelectedFiles == UNION {Closure(g): g \in Selected}
Protected == (IF ReclaimPinned THEN Closure(active) ELSE SelectedFiles)
             \cup (IF phase = "durable" THEN Closure(candidate) ELSE {})

Init == /\ active = 0
        /\ candidate = -1
        /\ phase = "idle"
        /\ durable = Closure(0)
        /\ pins = [r \in Readers |-> -1]
        /\ published = {0}
        /\ stalePublished = FALSE

Prepare(g) == /\ phase = "idle"
              /\ g \in 1..5
              /\ Parent(g) = active
              /\ g \notin published
              /\ candidate' = g
              /\ phase' = "staged"
              /\ UNCHANGED <<active, durable, pins, published, stalePublished>>

Flush == /\ phase = "staged"
         /\ durable' = durable \cup Closure(candidate)
         /\ phase' = "durable"
         /\ UNCHANGED <<active, candidate, pins, published, stalePublished>>

Publish == /\ phase = "durable" \/ (PublishEarly /\ phase = "staged")
           /\ SkipCAS \/ Parent(candidate) = active
           /\ active' = candidate
           /\ published' = published \cup {candidate}
           /\ stalePublished' = (stalePublished \/ Parent(candidate) # active)
           /\ candidate' = -1
           /\ phase' = "idle"
           /\ UNCHANGED <<durable, pins>>

(* Another writer completes its valid delete while the first is prepared.
   Its atomic action abstracts its own flush-before-selector sequence. *)
CompetingPublish == /\ active = 0
                    /\ candidate = 1
                    /\ phase \in {"staged", "durable"}
                    /\ active' = 5
                    /\ durable' = durable \cup Closure(5)
                    /\ published' = published \cup {5}
                    /\ UNCHANGED <<candidate, phase, pins, stalePublished>>

Discard == /\ phase # "idle"
           /\ candidate' = -1
           /\ phase' = "idle"
           /\ UNCHANGED <<active, durable, pins, published, stalePublished>>

Pin(r) == /\ pins[r] = -1
          /\ pins' = [pins EXCEPT ![r] = active]
          /\ UNCHANGED <<active, candidate, phase, durable, published, stalePublished>>
Unpin(r) == /\ pins[r] # -1
            /\ pins' = [pins EXCEPT ![r] = -1]
            /\ UNCHANGED <<active, candidate, phase, durable, published, stalePublished>>

Reclaim(f) == /\ f \in durable \ Protected
              /\ durable' = durable \ {f}
              /\ UNCHANGED <<active, candidate, phase, pins, published, stalePublished>>

Crash == /\ candidate' = -1
         /\ phase' = "idle"
         /\ pins' = [r \in Readers |-> -1]
         /\ UNCHANGED <<active, durable, published, stalePublished>>

Next == (\E g \in 1..5: Prepare(g)) \/ Flush \/ Publish \/ CompetingPublish
        \/ Discard \/ (\E r \in Readers: Pin(r) \/ Unpin(r))
        \/ (\E f \in Files: Reclaim(f)) \/ Crash
Spec == Init /\ [][Next]_vars

TypeOK == /\ active \in Gens
          /\ candidate \in {-1} \cup Gens
          /\ phase \in {"idle", "staged", "durable"}
          /\ (phase = "idle") = (candidate = -1)
          /\ durable \subseteq Files
          /\ pins \in [Readers -> ({-1} \cup Gens)]
          /\ published \subseteq Gens
          /\ active \in published
          /\ stalePublished \in BOOLEAN
ActiveClosureDurable == Closure(active) \subseteq durable
PinnedClosureRetained == \A r \in Readers:
                          pins[r] # -1 => Closure(pins[r]) \subseteq durable
TargetsRemainBound == \A g \in Selected: BoundTargets(g)
VisibilityMatchesLogical == \A g \in Selected: Logical(g) = Expected(g)
StaleNeverPublished == ~stalePublished

(* Reachability controls, intentionally false on the named path. *)
NoRepeatedReplacement == active # 2
NoCompaction == active # 4
=============================================================================
