------------------- MODULE HawDBImmutableRootBindings -------------------
EXTENDS FiniteSets

(* A sealed root deduplicates immutable content references, not physical
   manifest paths. Two live generations may name the same immutable object,
   and recovery must materialize both paths. *)
CONSTANT DropDuplicateContentBinding

CheckpointReferences == {"checkpoint", "shared-overflow"}
SharedReference == "shared-overflow"
ManifestBindings == {
    [path |-> "checkpoint.hawdb", reference |-> "checkpoint"],
    [path |-> "generations/overflow-11.hawdb", reference |-> SharedReference],
    [path |-> "generations/overflow-12.hawdb", reference |-> SharedReference]
}

BindingPaths(bindings) == {binding.path: binding \in bindings}
BindingReferences(bindings) == {binding.reference: binding \in bindings}
ManifestPaths == BindingPaths(ManifestBindings)

(* The negative control is the erroneous reference-level deduplication from
   #812: it retains one shared-content path while dropping the other. *)
BindingsAtSeal ==
    IF DropDuplicateContentBinding
       THEN {binding \in ManifestBindings:
                 binding.path # "generations/overflow-12.hawdb"}
       ELSE ManifestBindings

VARIABLES published, sealed, rootBindings, recovered, materializedBindings
vars == <<published, sealed, rootBindings, recovered, materializedBindings>>

Init ==
    /\ published = {}
    /\ sealed = FALSE
    /\ rootBindings = {}
    /\ recovered = FALSE
    /\ materializedBindings = {}

Publish(reference) ==
    /\ reference \in CheckpointReferences \ published
    /\ published' = published \cup {reference}
    /\ UNCHANGED <<sealed, rootBindings, recovered, materializedBindings>>

SealRoot ==
    /\ ~sealed
    /\ CheckpointReferences \subseteq published
    /\ sealed' = TRUE
    /\ rootBindings' = BindingsAtSeal
    /\ UNCHANGED <<published, recovered, materializedBindings>>

Recover ==
    /\ sealed
    /\ ~recovered
    /\ BindingReferences(rootBindings) \subseteq published
    /\ recovered' = TRUE
    /\ materializedBindings' = rootBindings
    /\ UNCHANGED <<published, sealed, rootBindings>>

Next ==
    (\E reference \in CheckpointReferences: Publish(reference))
    \/ SealRoot
    \/ Recover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ published \subseteq CheckpointReferences
    /\ sealed \in BOOLEAN
    /\ rootBindings \subseteq ManifestBindings
    /\ recovered \in BOOLEAN
    /\ materializedBindings \subseteq ManifestBindings

(* Reference coverage alone is insufficient: both overflow paths share the
   same reference, so it remains true after the broken deduplication. *)
RootReferencesHaveBindings ==
    sealed => CheckpointReferences \subseteq BindingReferences(rootBindings)

RootBindsEveryManifestPath ==
    sealed => BindingPaths(rootBindings) = ManifestPaths

RecoveredRootMaterializesEveryPath ==
    recovered => BindingPaths(materializedBindings) = ManifestPaths
=============================================================================
