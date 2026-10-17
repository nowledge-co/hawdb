---------------- MODULE HawDBProjectionFreshness ----------------
EXTENDS Integers, FiniteSets
CONSTANTS CountOwnerOnly, IgnoreFloor
VARIABLES head, floor, retained, relevant, checkpoint
vars == <<head, floor, retained, relevant, checkpoint>>
Max(s) == CHOOSE e \in s: \A other \in s: other <= e
Image == [head |-> head, floor |-> floor, retained |-> retained, relevant |-> relevant]
Init == /\ head = 0 /\ floor = 0 /\ retained = {} /\ relevant = {}
        /\ checkpoint = Image
Commit(isRelevant) == /\ head < 3 /\ isRelevant \in BOOLEAN
 /\ head' = head + 1 /\ retained' = retained \cup {head + 1}
 /\ relevant' = (IF isRelevant THEN relevant \cup {head + 1} ELSE relevant)
 /\ UNCHANGED <<floor, checkpoint>>
Gap == /\ head < 3 /\ head' = head + 1 /\ floor' = head + 1
       /\ retained' = {} /\ relevant' = {} /\ UNCHANGED checkpoint
Trim == /\ retained # {}
 /\ LET e == CHOOSE e \in retained: \A other \in retained: e <= other
    IN /\ floor' = e /\ retained' = retained \ {e} /\ relevant' = relevant \ {e}
 /\ UNCHANGED <<head, checkpoint>>
Checkpoint == /\ checkpoint' = Image /\ UNCHANGED <<head, floor, retained, relevant>>
Recover == /\ head' = checkpoint.head /\ floor' = checkpoint.floor
 /\ retained' = checkpoint.retained /\ relevant' = checkpoint.relevant
 /\ UNCHANGED checkpoint
Next == (\E isRelevant \in BOOLEAN: Commit(isRelevant)) \/ Gap \/ Trim \/ Checkpoint \/ Recover
Required == Max({IF IgnoreFloor THEN 0 ELSE floor} \cup
                (IF CountOwnerOnly THEN retained ELSE relevant))
Expected == Max({floor} \cup relevant)
TypeOK == /\ head \in 0..3 /\ floor \in 0..head
 /\ retained \subseteq (floor + 1)..head /\ relevant \subseteq retained
 /\ checkpoint.head \in 0..3 /\ checkpoint.floor \in 0..checkpoint.head
 /\ checkpoint.retained \subseteq (checkpoint.floor + 1)..checkpoint.head
 /\ checkpoint.relevant \subseteq checkpoint.retained
NoFalseStaleness == Required <= Expected
NoMissedProjection == Required >= Expected
Spec == Init /\ [][Next]_vars
=============================================================================
