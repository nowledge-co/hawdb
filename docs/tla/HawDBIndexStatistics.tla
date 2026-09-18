-------------------- MODULE HawDBIndexStatistics --------------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* One index sample is an immutable view of a prior complete index state.   *)
(* Canonical key mutations only advance the epoch and churn counter. A       *)
(* resample pins one candidate state and publishes it only if no intervening *)
(* mutation changed the source epoch; otherwise the candidate is discarded. *)
(* An explicit direct resample stays ungated. A caller-owned background scan *)
(* starts only after admission and releases its permit on every termination. *)
(***************************************************************************)

CONSTANTS Nodes, Keys, MaxFreshUpdates, MaxEpoch

ASSUME /\ Nodes /= {}
       /\ Keys /= {}
       /\ IsFiniteSet(Nodes)
       /\ IsFiniteSet(Keys)
       /\ MaxFreshUpdates \in Nat
       /\ MaxEpoch \in Nat

EntriesUniverse == [node : Nodes, key : Keys]

VARIABLES entries, sampleEntries, epoch, sampleEpoch, updates,
          plannerUsesSample, refreshing, candidateEntries, candidateEpoch,
          refreshMode, backgroundPermit

vars == <<entries, sampleEntries, epoch, sampleEpoch, updates,
          plannerUsesSample, refreshing, candidateEntries, candidateEpoch,
          refreshMode, backgroundPermit>>

IndexSize(indexEntries) == Cardinality(indexEntries)

UniqueValues(indexEntries) ==
    Cardinality({key \in Keys : \E entry \in indexEntries : entry.key = key})

SampleUsable ==
    /\ IndexSize(sampleEntries) > 0
    /\ updates <= MaxFreshUpdates

Init ==
    /\ entries = {}
    /\ sampleEntries = {}
    /\ epoch = 0
    /\ sampleEpoch = 0
    /\ updates = 0
    /\ plannerUsesSample = FALSE
    /\ refreshing = FALSE
    /\ candidateEntries = {}
    /\ candidateEpoch = 0
    /\ refreshMode = "none"
    /\ backgroundPermit = FALSE

Insert ==
    /\ epoch < MaxEpoch
    /\ \E entry \in EntriesUniverse \ entries:
        /\ entries' = entries \cup {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch, refreshing,
                       candidateEntries, candidateEpoch, refreshMode,
                       backgroundPermit>>

Delete ==
    /\ epoch < MaxEpoch
    /\ \E entry \in entries:
        /\ entries' = entries \ {entry}
        /\ epoch' = epoch + 1
        /\ updates' = updates + 1
        /\ plannerUsesSample' = FALSE
        /\ UNCHANGED <<sampleEntries, sampleEpoch, refreshing,
                       candidateEntries, candidateEpoch, refreshMode,
                       backgroundPermit>>

StartDirectResample ==
    /\ ~refreshing
    /\ ~backgroundPermit
    /\ refreshing' = TRUE
    /\ candidateEntries' = entries
    /\ candidateEpoch' = epoch
    /\ plannerUsesSample' = FALSE
    /\ refreshMode' = "direct"
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   backgroundPermit>>

AdmitBackgroundResample ==
    /\ ~refreshing
    /\ ~backgroundPermit
    /\ backgroundPermit' = TRUE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   plannerUsesSample, refreshing, candidateEntries,
                   candidateEpoch, refreshMode>>

StartBackgroundResample ==
    /\ ~refreshing
    /\ backgroundPermit
    /\ refreshing' = TRUE
    /\ candidateEntries' = entries
    /\ candidateEpoch' = epoch
    /\ plannerUsesSample' = FALSE
    /\ refreshMode' = "background"
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   backgroundPermit>>

CancelBackgroundResample ==
    /\ ~refreshing
    /\ backgroundPermit
    /\ backgroundPermit' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   plannerUsesSample, refreshing, candidateEntries,
                   candidateEpoch, refreshMode>>

PublishResample ==
    /\ refreshing
    /\ candidateEpoch = epoch
    /\ sampleEntries' = candidateEntries
    /\ sampleEpoch' = candidateEpoch
    /\ updates' = 0
    /\ plannerUsesSample' = FALSE
    /\ refreshing' = FALSE
    /\ refreshMode' = "none"
    /\ backgroundPermit' = FALSE
    /\ UNCHANGED <<entries, epoch, candidateEntries, candidateEpoch>>

AbortResample ==
    /\ refreshing
    /\ candidateEpoch # epoch
    /\ refreshing' = FALSE
    /\ plannerUsesSample' = FALSE
    /\ refreshMode' = "none"
    /\ backgroundPermit' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   candidateEntries, candidateEpoch>>

FailResample ==
    /\ refreshing
    /\ refreshing' = FALSE
    /\ plannerUsesSample' = FALSE
    /\ refreshMode' = "none"
    /\ backgroundPermit' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   candidateEntries, candidateEpoch>>

PlanWithSample ==
    /\ SampleUsable
    /\ plannerUsesSample' = TRUE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   refreshing, candidateEntries, candidateEpoch, refreshMode,
                   backgroundPermit>>

PlanWithoutSample ==
    /\ plannerUsesSample' = FALSE
    /\ UNCHANGED <<entries, sampleEntries, epoch, sampleEpoch, updates,
                   refreshing, candidateEntries, candidateEpoch, refreshMode,
                   backgroundPermit>>

Next == Insert \/ Delete \/ StartDirectResample \/ AdmitBackgroundResample
        \/ StartBackgroundResample \/ CancelBackgroundResample
        \/ PublishResample \/ AbortResample \/ FailResample \/ PlanWithSample
        \/ PlanWithoutSample

TypeOK ==
    /\ entries \subseteq EntriesUniverse
    /\ sampleEntries \subseteq EntriesUniverse
    /\ epoch \in Nat
    /\ sampleEpoch \in Nat
    /\ sampleEpoch <= epoch
    /\ epoch <= MaxEpoch
    /\ updates \in Nat
    /\ plannerUsesSample \in BOOLEAN
    /\ refreshing \in BOOLEAN
    /\ candidateEntries \subseteq EntriesUniverse
    /\ candidateEpoch \in Nat
    /\ candidateEpoch <= epoch
    /\ refreshMode \in {"none", "direct", "background"}
    /\ backgroundPermit \in BOOLEAN

SampleCountersAreValid ==
    UniqueValues(sampleEntries) <= IndexSize(sampleEntries)

UpdatesTrackSampleAge == updates = epoch - sampleEpoch

ZeroChurnSampleIsExact == updates = 0 => sampleEntries = entries

PlannerUsesOnlyFreshSamples == plannerUsesSample => SampleUsable

PublishedSampleNeverUsesFutureState == sampleEpoch <= epoch

BackgroundResampleRequiresPermit ==
    refreshMode = "background" => backgroundPermit

DirectResampleNeverOwnsBackgroundPermit ==
    refreshMode = "direct" => ~backgroundPermit

IdleRefreshHasNoMode == ~refreshing => refreshMode = "none"

Spec == Init /\ [][Next]_vars

=============================================================================
