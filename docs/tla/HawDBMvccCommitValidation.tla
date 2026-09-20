-------------------- MODULE HawDBMvccCommitValidation --------------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* Bounded snapshot-isolation model for the storage-owned VersionIndex.     *)
(* Transaction workspaces capture a published epoch and only the sequencer   *)
(* may validate, make a batch durable, and publish its versions.             *)
(*                                                                            *)
(* The model intentionally permits write skew: it validates write identities *)
(* rather than predicate reads. It proves first-committer-wins for a shared  *)
(* identity, permits disjoint identities, keeps readers on their selected    *)
(* snapshot, and retains delete stamps until pinned old readers are gone.    *)
(***************************************************************************)

CONSTANTS Transactions, Readers, Keys, MaxEpoch, MutatePublishBeforeValidation

ASSUME /\ Transactions # {}
       /\ Readers # {}
       /\ Keys # {}
       /\ MaxEpoch \in Nat \ {0}
       /\ MutatePublishBeforeValidation \in BOOLEAN

TxPhases == {"idle", "active", "validated", "durable", "committed", "conflict"}
WriteModes == {"none", "update", "delete"}
Epochs == 0..MaxEpoch

VARIABLES publishedEpoch,
          durableEpoch,
          versions,
          durableVersions,
          tombstones,
          durableTombstones,
          txPhase,
          readEpoch,
          snapshotVersions,
          writeKey,
          writeMode,
          readerActive,
          readerEpoch,
          readerVersions,
          readerTombstones,
          lastPublishValidated

vars == <<publishedEpoch,
          durableEpoch,
          versions,
          durableVersions,
          tombstones,
          durableTombstones,
          txPhase,
          readEpoch,
          snapshotVersions,
          writeKey,
          writeMode,
          readerActive,
          readerEpoch,
          readerVersions,
          readerTombstones,
          lastPublishValidated>>

EmptyVersions == [key \in Keys |-> 0]

Init ==
    /\ publishedEpoch = 0
    /\ durableEpoch = 0
    /\ versions = EmptyVersions
    /\ durableVersions = EmptyVersions
    /\ tombstones = EmptyVersions
    /\ durableTombstones = EmptyVersions
    /\ txPhase = [tx \in Transactions |-> "idle"]
    /\ readEpoch = [tx \in Transactions |-> 0]
    /\ snapshotVersions = [tx \in Transactions |-> EmptyVersions]
    /\ writeKey = [tx \in Transactions |-> "none"]
    /\ writeMode = [tx \in Transactions |-> "none"]
    /\ readerActive = [reader \in Readers |-> FALSE]
    /\ readerEpoch = [reader \in Readers |-> 0]
    /\ readerVersions = [reader \in Readers |-> EmptyVersions]
    /\ readerTombstones = [reader \in Readers |-> EmptyVersions]
    /\ lastPublishValidated = TRUE

Begin(tx) ==
    /\ txPhase[tx] \in {"idle", "committed", "conflict"}
    /\ publishedEpoch < MaxEpoch
    /\ txPhase' = [txPhase EXCEPT ![tx] = "active"]
    /\ readEpoch' = [readEpoch EXCEPT ![tx] = publishedEpoch]
    /\ snapshotVersions' = [snapshotVersions EXCEPT ![tx] = versions]
    /\ writeKey' = [writeKey EXCEPT ![tx] = "none"]
    /\ writeMode' = [writeMode EXCEPT ![tx] = "none"]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, readerActive, readerEpoch,
                    readerVersions, readerTombstones, lastPublishValidated>>

StageWrite(tx, key, mode) ==
    /\ txPhase[tx] = "active"
    /\ writeKey[tx] = "none"
    /\ key \in Keys
    /\ mode \in {"update", "delete"}
    /\ writeKey' = [writeKey EXCEPT ![tx] = key]
    /\ writeMode' = [writeMode EXCEPT ![tx] = mode]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, txPhase, readEpoch,
                    snapshotVersions, readerActive, readerEpoch, readerVersions,
                    readerTombstones, lastPublishValidated>>

Validate(tx) ==
    /\ txPhase[tx] = "active"
    /\ writeKey[tx] \in Keys
    /\ versions[writeKey[tx]] <= readEpoch[tx]
    /\ \A other \in Transactions:
        other = tx \/ txPhase[other] \notin {"validated", "durable"}
    /\ txPhase' = [txPhase EXCEPT ![tx] = "validated"]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, readEpoch, snapshotVersions,
                    writeKey, writeMode, readerActive, readerEpoch, readerVersions,
                    readerTombstones, lastPublishValidated>>

RejectConflict(tx) ==
    /\ txPhase[tx] = "active"
    /\ writeKey[tx] \in Keys
    /\ versions[writeKey[tx]] > readEpoch[tx]
    /\ txPhase' = [txPhase EXCEPT ![tx] = "conflict"]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, readEpoch, snapshotVersions,
                    writeKey, writeMode, readerActive, readerEpoch, readerVersions,
                    readerTombstones, lastPublishValidated>>

DurableCommit(tx) ==
    /\ txPhase[tx] = "validated"
    /\ durableEpoch = publishedEpoch
    /\ publishedEpoch < MaxEpoch
    /\ durableEpoch' = publishedEpoch + 1
    /\ durableVersions' =
        [durableVersions EXCEPT ![writeKey[tx]] = publishedEpoch + 1]
    /\ durableTombstones' =
        [durableTombstones EXCEPT
            ![writeKey[tx]] =
                IF writeMode[tx] = "delete" THEN publishedEpoch + 1 ELSE 0]
    /\ txPhase' = [txPhase EXCEPT ![tx] = "durable"]
    /\ UNCHANGED <<publishedEpoch, versions, tombstones, readEpoch,
                    snapshotVersions, writeKey, writeMode, readerActive,
                    readerEpoch, readerVersions, readerTombstones,
                    lastPublishValidated>>

Publish(tx) ==
    /\ txPhase[tx] = "durable"
    /\ publishedEpoch' = durableEpoch
    /\ versions' = durableVersions
    /\ tombstones' = durableTombstones
    /\ txPhase' = [txPhase EXCEPT ![tx] = "committed"]
    /\ lastPublishValidated' = TRUE
    /\ UNCHANGED <<durableEpoch, durableVersions, durableTombstones, readEpoch,
                    snapshotVersions, writeKey, writeMode, readerActive,
                    readerEpoch, readerVersions, readerTombstones>>

PublishBeforeValidation(tx) ==
    /\ MutatePublishBeforeValidation
    /\ txPhase[tx] = "active"
    /\ writeKey[tx] \in Keys
    /\ publishedEpoch < MaxEpoch
    /\ publishedEpoch' = publishedEpoch + 1
    /\ versions' = [versions EXCEPT ![writeKey[tx]] = publishedEpoch + 1]
    /\ tombstones' =
        [tombstones EXCEPT
            ![writeKey[tx]] =
                IF writeMode[tx] = "delete" THEN publishedEpoch + 1 ELSE 0]
    /\ txPhase' = [txPhase EXCEPT ![tx] = "committed"]
    /\ lastPublishValidated' = FALSE
    /\ UNCHANGED <<durableEpoch, durableVersions, durableTombstones, readEpoch,
                    snapshotVersions, writeKey, writeMode, readerActive,
                    readerEpoch, readerVersions, readerTombstones>>

CrashRecover ==
    /\ publishedEpoch' = durableEpoch
    /\ versions' = durableVersions
    /\ tombstones' = durableTombstones
    /\ txPhase' = [tx \in Transactions |-> "idle"]
    /\ writeKey' = [tx \in Transactions |-> "none"]
    /\ writeMode' = [tx \in Transactions |-> "none"]
    /\ lastPublishValidated' = TRUE
    /\ UNCHANGED <<durableEpoch, durableVersions, durableTombstones, readEpoch,
                    snapshotVersions, readerActive, readerEpoch, readerVersions,
                    readerTombstones>>

BeginReader(reader) ==
    /\ ~readerActive[reader]
    /\ readerActive' = [readerActive EXCEPT ![reader] = TRUE]
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = publishedEpoch]
    /\ readerVersions' = [readerVersions EXCEPT ![reader] = versions]
    /\ readerTombstones' = [readerTombstones EXCEPT ![reader] = tombstones]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, txPhase, readEpoch,
                    snapshotVersions, writeKey, writeMode, lastPublishValidated>>

EndReader(reader) ==
    /\ readerActive[reader]
    /\ readerActive' = [readerActive EXCEPT ![reader] = FALSE]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    tombstones, durableTombstones, txPhase, readEpoch,
                    snapshotVersions, writeKey, writeMode, readerEpoch,
                    readerVersions, readerTombstones, lastPublishValidated>>

ReclaimTombstone(key) ==
    /\ tombstones[key] # 0
    /\ \A reader \in Readers:
        ~readerActive[reader] \/ readerEpoch[reader] > tombstones[key]
    /\ \A tx \in Transactions:
        txPhase[tx] \notin {"active", "validated", "durable"} \/
            readEpoch[tx] > tombstones[key]
    /\ tombstones' = [tombstones EXCEPT ![key] = 0]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, versions, durableVersions,
                    durableTombstones, txPhase, readEpoch, snapshotVersions,
                    writeKey, writeMode, readerActive, readerEpoch, readerVersions,
                    readerTombstones, lastPublishValidated>>

Next ==
    \/ \E tx \in Transactions: Begin(tx)
    \/ \E tx \in Transactions, key \in Keys, mode \in {"update", "delete"}:
        StageWrite(tx, key, mode)
    \/ \E tx \in Transactions: Validate(tx)
    \/ \E tx \in Transactions: RejectConflict(tx)
    \/ \E tx \in Transactions: DurableCommit(tx)
    \/ \E tx \in Transactions: Publish(tx)
    \/ \E tx \in Transactions: PublishBeforeValidation(tx)
    \/ CrashRecover
    \/ \E reader \in Readers: BeginReader(reader)
    \/ \E reader \in Readers: EndReader(reader)
    \/ \E key \in Keys: ReclaimTombstone(key)

TypeOK ==
    /\ publishedEpoch \in Epochs
    /\ durableEpoch \in Epochs
    /\ versions \in [Keys -> Epochs]
    /\ durableVersions \in [Keys -> Epochs]
    /\ tombstones \in [Keys -> Epochs]
    /\ durableTombstones \in [Keys -> Epochs]
    /\ txPhase \in [Transactions -> TxPhases]
    /\ readEpoch \in [Transactions -> Epochs]
    /\ snapshotVersions \in [Transactions -> [Keys -> Epochs]]
    /\ writeKey \in [Transactions -> (Keys \union {"none"})]
    /\ writeMode \in [Transactions -> WriteModes]
    /\ readerActive \in [Readers -> BOOLEAN]
    /\ readerEpoch \in [Readers -> Epochs]
    /\ readerVersions \in [Readers -> [Keys -> Epochs]]
    /\ readerTombstones \in [Readers -> [Keys -> Epochs]]
    /\ lastPublishValidated \in BOOLEAN

DurableBeforePublication == publishedEpoch <= durableEpoch

PublishedVersionsAreDurable ==
    /\ publishedEpoch <= durableEpoch
    /\ \A key \in Keys: versions[key] <= publishedEpoch
    /\ \A key \in Keys: tombstones[key] <= publishedEpoch

PublishedStateWasValidated == lastPublishValidated

ValidatedWritesAreCurrent ==
    \A tx \in Transactions:
        txPhase[tx] \in {"validated", "durable", "committed"} =>
            versions[writeKey[tx]] <= readEpoch[tx] \/ txPhase[tx] = "committed"

ConflictRequiresStaleSnapshot ==
    \A tx \in Transactions:
        txPhase[tx] = "conflict" => versions[writeKey[tx]] > readEpoch[tx]

ReadersKeepSelectedSnapshot ==
    \A reader \in Readers:
        readerActive[reader] =>
            /\ \A key \in Keys: readerVersions[reader][key] <= readerEpoch[reader]
            /\ \A key \in Keys:
                readerTombstones[reader][key] <= readerEpoch[reader]

PinnedTombstoneVersionsAreRetained ==
    \A reader \in Readers, key \in Keys:
        readerActive[reader] /\ readerTombstones[reader][key] # 0 /\
            readerEpoch[reader] <= readerTombstones[reader][key] =>
            versions[key] >= readerTombstones[reader][key]

Spec == Init /\ [][Next]_vars

=============================================================================
