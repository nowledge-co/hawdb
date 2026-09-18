----------------- MODULE HawDBSparseRelationalLivePreparation -----------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A schema-derived plan seeds one bounded live workspace. Unpublished DML  *)
(* preparation may discover additional exact replay keys and authoritative  *)
(* constraint probes. The caller repeatedly hydrates only missing required  *)
(* keys until the set is closed, or rejects when the entry bound prevents   *)
(* closure. Preparation never publishes canonical state or WAL.             *)
(***************************************************************************)

Keys == {"k1", "k2"}
MaxWorkspaceEntries == 2

Required(replay, constraints) == replay \cup constraints

VARIABLES
    phase,
    initialAccess,
    replayAccess,
    constraintAccess,
    suppliedAccess,
    publicationOccurred

vars == <<
    phase,
    initialAccess,
    replayAccess,
    constraintAccess,
    suppliedAccess,
    publicationOccurred
>>

Init ==
    /\ phase = "idle"
    /\ initialAccess = {}
    /\ replayAccess = {}
    /\ constraintAccess = {}
    /\ suppliedAccess = {}
    /\ publicationOccurred = FALSE

Begin(initial, replay, constraints, supplied) ==
    /\ phase = "idle"
    /\ initial \subseteq Keys
    /\ replay \subseteq Keys
    /\ constraints \subseteq Keys
    /\ supplied \subseteq Keys
    /\ initialAccess' = initial
    /\ replayAccess' = replay
    /\ constraintAccess' = constraints
    /\ suppliedAccess' = supplied
    /\ publicationOccurred' = FALSE
    /\ IF initial \subseteq supplied
             /\ Cardinality(supplied) <= MaxWorkspaceEntries
          THEN phase' = "prepared"
          ELSE phase' = "rejected"

CheckClosure ==
    /\ phase = "prepared"
    /\ IF Required(replayAccess, constraintAccess) \subseteq suppliedAccess
          THEN phase' = "closed"
          ELSE phase' = "needs_hydration"
    /\ UNCHANGED <<
        initialAccess, replayAccess, constraintAccess, suppliedAccess,
        publicationOccurred
        >>

HydrateMissing ==
    /\ phase = "needs_hydration"
    /\ \E key \in Required(replayAccess, constraintAccess) \ suppliedAccess:
          /\ IF Cardinality(suppliedAccess) < MaxWorkspaceEntries
                THEN /\ phase' = "prepared"
                     /\ suppliedAccess' = suppliedAccess \cup {key}
                ELSE /\ phase' = "rejected"
                     /\ suppliedAccess' = suppliedAccess
    /\ UNCHANGED <<
        initialAccess, replayAccess, constraintAccess, publicationOccurred
        >>

Next ==
    \/ \E initial \in SUBSET Keys,
          replay \in SUBSET Keys,
          constraints \in SUBSET Keys,
          supplied \in SUBSET Keys:
          Begin(initial, replay, constraints, supplied)
    \/ CheckClosure
    \/ HydrateMissing

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {
        "idle", "prepared", "needs_hydration", "closed", "rejected"
        }
    /\ initialAccess \subseteq Keys
    /\ replayAccess \subseteq Keys
    /\ constraintAccess \subseteq Keys
    /\ suppliedAccess \subseteq Keys
    /\ publicationOccurred \in BOOLEAN

PreparationNeverPublishes ==
    publicationOccurred = FALSE

PreparedWorkspaceContainsInitialPlan ==
    phase \in {"prepared", "needs_hydration", "closed"} =>
        initialAccess \subseteq suppliedAccess

PreparedWorkspaceIsBounded ==
    phase \in {"prepared", "needs_hydration", "closed"} =>
        Cardinality(suppliedAccess) <= MaxWorkspaceEntries

ClosedWorkspaceCoversReplay ==
    phase = "closed" => replayAccess \subseteq suppliedAccess

ClosedWorkspaceCoversConstraints ==
    phase = "closed" => constraintAccess \subseteq suppliedAccess

RejectedWorkspaceNeverPublishes ==
    phase = "rejected" => publicationOccurred = FALSE

=============================================================================
