-------------------- MODULE HawDBSparseRelationalActivation -------------------
EXTENDS Naturals

(***************************************************************************)
(* OutOfCore plus Authoritative opens canonical relation metadata without   *)
(* decoding checkpoint rows. Row and index recovery may complete in either  *)
(* order, but the handle activates only when both immutable views match the  *)
(* database epoch. The first later write advances state and both views       *)
(* together through the separately modeled sparse live-commit protocol.     *)
(***************************************************************************)

BaseEpoch == 1
RecoveredEpoch == 2
MaxEpoch == 3

VARIABLES
    phase,
    walPresent,
    metadataOnly,
    databaseEpoch,
    rowViewEpoch,
    indexViewEpoch,
    active,
    firstLiveCommit

vars == <<
    phase,
    walPresent,
    metadataOnly,
    databaseEpoch,
    rowViewEpoch,
    indexViewEpoch,
    active,
    firstLiveCommit
>>

Init ==
    /\ phase = "idle"
    /\ walPresent = FALSE
    /\ metadataOnly = FALSE
    /\ databaseEpoch = 0
    /\ rowViewEpoch = 0
    /\ indexViewEpoch = 0
    /\ active = FALSE
    /\ firstLiveCommit = FALSE

MountCheckpoint(hasWal) ==
    /\ phase = "idle"
    /\ hasWal \in BOOLEAN
    /\ phase' = IF hasWal THEN "recovering" ELSE "ready"
    /\ walPresent' = hasWal
    /\ metadataOnly' = TRUE
    /\ databaseEpoch' = IF hasWal THEN RecoveredEpoch ELSE BaseEpoch
    /\ rowViewEpoch' = BaseEpoch
    /\ indexViewEpoch' = BaseEpoch
    /\ UNCHANGED <<active, firstLiveCommit>>

RecoverRows ==
    /\ phase = "recovering"
    /\ walPresent
    /\ rowViewEpoch = BaseEpoch
    /\ rowViewEpoch' = RecoveredEpoch
    /\ UNCHANGED <<
        phase, walPresent, metadataOnly, databaseEpoch, indexViewEpoch, active,
        firstLiveCommit
        >>

RecoverIndexes ==
    /\ phase = "recovering"
    /\ walPresent
    /\ indexViewEpoch = BaseEpoch
    /\ indexViewEpoch' = RecoveredEpoch
    /\ UNCHANGED <<
        phase, walPresent, metadataOnly, databaseEpoch, rowViewEpoch, active,
        firstLiveCommit
        >>

RecoveryReady ==
    /\ phase = "recovering"
    /\ rowViewEpoch = databaseEpoch
    /\ indexViewEpoch = databaseEpoch
    /\ phase' = "ready"
    /\ UNCHANGED <<
        walPresent, metadataOnly, databaseEpoch, rowViewEpoch, indexViewEpoch,
        active, firstLiveCommit
        >>

RejectMissingView ==
    /\ phase \in {"recovering", "ready"}
    /\ \/ ~metadataOnly
       \/ rowViewEpoch # databaseEpoch
       \/ indexViewEpoch # databaseEpoch
    /\ phase' = "rejected"
    /\ active' = FALSE
    /\ UNCHANGED <<
        walPresent, metadataOnly, databaseEpoch, rowViewEpoch, indexViewEpoch,
        firstLiveCommit
        >>

Activate ==
    /\ phase = "ready"
    /\ metadataOnly
    /\ rowViewEpoch = databaseEpoch
    /\ indexViewEpoch = databaseEpoch
    /\ phase' = "active"
    /\ active' = TRUE
    /\ UNCHANGED <<
        walPresent, metadataOnly, databaseEpoch, rowViewEpoch, indexViewEpoch,
        firstLiveCommit
        >>

CommitFirstLiveWrite ==
    /\ phase = "active"
    /\ active
    /\ databaseEpoch \in {BaseEpoch, RecoveredEpoch}
    /\ phase' = "live"
    /\ databaseEpoch' = databaseEpoch + 1
    /\ rowViewEpoch' = databaseEpoch + 1
    /\ indexViewEpoch' = databaseEpoch + 1
    /\ firstLiveCommit' = TRUE
    /\ UNCHANGED <<walPresent, metadataOnly, active>>

Next ==
    \/ \E hasWal \in BOOLEAN: MountCheckpoint(hasWal)
    \/ RecoverRows
    \/ RecoverIndexes
    \/ RecoveryReady
    \/ RejectMissingView
    \/ Activate
    \/ CommitFirstLiveWrite

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "recovering", "ready", "active", "live", "rejected"}
    /\ walPresent \in BOOLEAN
    /\ metadataOnly \in BOOLEAN
    /\ databaseEpoch \in 0..MaxEpoch
    /\ rowViewEpoch \in 0..MaxEpoch
    /\ indexViewEpoch \in 0..MaxEpoch
    /\ active \in BOOLEAN
    /\ firstLiveCommit \in BOOLEAN

ActiveViewIdentityIsExact ==
    active =>
        /\ metadataOnly
        /\ rowViewEpoch = databaseEpoch
        /\ indexViewEpoch = databaseEpoch

RejectedOpenDoesNotActivate ==
    phase = "rejected" => ~active

LiveCommitPreservesMetadataOnlyState ==
    firstLiveCommit => metadataOnly

FirstLiveCommitPublishesOneEpoch ==
    firstLiveCommit =>
        /\ databaseEpoch \in {BaseEpoch + 1, RecoveredEpoch + 1}
        /\ rowViewEpoch = databaseEpoch
        /\ indexViewEpoch = databaseEpoch

=============================================================================
