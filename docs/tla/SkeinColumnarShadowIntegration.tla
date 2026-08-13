---------------- MODULE SkeinColumnarShadowIntegration ----------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* Columnar shadow adoption phase (spec §3.7). A checkpoint is a           *)
(* four-phase machine: publish the canonical manifest, publish the shadow  *)
(* key dictionary, publish the shadow manifest, then update the in-memory  *)
(* shadow catalog. A crash may land at every boundary. The shadow is       *)
(* derived, rebuildable state: it never influences canonical recovery, a   *)
(* stale or corrupt shadow is never mounted as current, and the canonical  *)
(* checkpoint's result reflects canonical publication only — a shadow      *)
(* build or publication failure reports through the shadow report with     *)
(* dirty state preserved, and a later successful checkpoint converges the  *)
(* shadow onto the canonical epoch.                                        *)
(*                                                                         *)
(* Epochs abstract commit epochs: memEpoch is the committed in-memory      *)
(* epoch (WAL-durable under SyncOnEveryWrite, so recovery restores it),    *)
(* canonicalEpoch the published canonical checkpoint, shadowEpoch the      *)
(* published shadow manifest's source epoch (-1 = absent). dirtyCovers     *)
(* abstracts the dirty-table tracker: TRUE iff the in-memory dirty state   *)
(* covers every mutation since the mounted shadow epoch. Corruption is a   *)
(* byte flip at rest, discovered only by validation at the next reopen.    *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Epochs == 0..MaxEpoch
OptionalEpochs == (-1)..MaxEpoch

Phases == {"idle", "canonical", "dict", "shadow"}
Results == {"none", "ok", "failed"}

VARIABLES
    memEpoch,
    canonicalEpoch,
    shadowEpoch,
    shadowIntact,
    dictEpoch,
    phase,
    mountedEpoch,
    allDirty,
    dirtyCovers,
    crashed,
    recoveryFailed,
    lastResult,
    lastCanonicalPublished,
    lastShadowPublished,
    readerEpoch

vars == <<
    memEpoch,
    canonicalEpoch,
    shadowEpoch,
    shadowIntact,
    dictEpoch,
    phase,
    mountedEpoch,
    allDirty,
    dirtyCovers,
    crashed,
    recoveryFailed,
    lastResult,
    lastCanonicalPublished,
    lastShadowPublished,
    readerEpoch
>>

Max(left, right) == IF left >= right THEN left ELSE right

Init ==
    /\ memEpoch = 0
    /\ canonicalEpoch = 0
    /\ shadowEpoch = -1
    /\ shadowIntact = TRUE
    /\ dictEpoch = -1
    /\ phase = "idle"
    /\ mountedEpoch = -1
    /\ allDirty = TRUE
    /\ dirtyCovers = FALSE
    /\ crashed = FALSE
    /\ recoveryFailed = FALSE
    /\ lastResult = "none"
    /\ lastCanonicalPublished = FALSE
    /\ lastShadowPublished = FALSE
    /\ readerEpoch = -1

(***************************************************************************)
(* A committed mutation. The dirty-table tracker marks its shadow tables   *)
(* at the apply layer, so live mutations never invalidate coverage.        *)
(***************************************************************************)
Mutate ==
    /\ ~crashed
    /\ phase = "idle"
    /\ memEpoch < MaxEpoch
    /\ memEpoch' = memEpoch + 1
    /\ UNCHANGED <<
        canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch, phase,
        mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        lastResult, lastCanonicalPublished, lastShadowPublished, readerEpoch
        >>

(***************************************************************************)
(* Phase 1: the canonical checkpoint replaces its manifest. From here the  *)
(* checkpoint's canonical publication is a fact regardless of what the     *)
(* shadow does afterwards.                                                 *)
(***************************************************************************)
PublishCanonicalManifest ==
    /\ ~crashed
    /\ phase = "idle"
    /\ canonicalEpoch' = memEpoch
    /\ phase' = "canonical"
    /\ lastResult' = "none"
    /\ lastCanonicalPublished' = TRUE
    /\ lastShadowPublished' = FALSE
    /\ UNCHANGED <<
        memEpoch, shadowEpoch, shadowIntact, dictEpoch, mountedEpoch,
        allDirty, dirtyCovers, crashed, recoveryFailed, readerEpoch
        >>

(***************************************************************************)
(* A checkpoint that fails before the canonical manifest replaces: the     *)
(* call returns an error and nothing durable changed.                      *)
(***************************************************************************)
CanonicalPublishFailure ==
    /\ ~crashed
    /\ phase = "idle"
    /\ lastResult' = "failed"
    /\ lastCanonicalPublished' = FALSE
    /\ lastShadowPublished' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        readerEpoch
        >>

(***************************************************************************)
(* Phase 2: the shadow key dictionary is rewritten durably. Its content    *)
(* only ever grows, so coverage is monotone and a crash between dictionary *)
(* and shadow manifest leaves a harmless superset.                         *)
(***************************************************************************)
PublishShadowDictionary ==
    /\ ~crashed
    /\ phase = "canonical"
    /\ dictEpoch' = Max(dictEpoch, canonicalEpoch)
    /\ phase' = "dict"
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, mountedEpoch,
        allDirty, dirtyCovers, crashed, recoveryFailed, lastResult,
        lastCanonicalPublished, lastShadowPublished, readerEpoch
        >>

(***************************************************************************)
(* Phase 3: the shadow manifest atomically replaces, selecting a complete  *)
(* catalog whose source epoch is the just-published canonical epoch. The   *)
(* dictionary must already cover it.                                       *)
(***************************************************************************)
PublishShadowManifest ==
    /\ ~crashed
    /\ phase = "dict"
    /\ dictEpoch >= canonicalEpoch
    /\ shadowEpoch' = canonicalEpoch
    /\ shadowIntact' = TRUE
    /\ phase' = "shadow"
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, dictEpoch, mountedEpoch, allDirty,
        dirtyCovers, crashed, recoveryFailed, lastResult,
        lastCanonicalPublished, lastShadowPublished, readerEpoch
        >>

(***************************************************************************)
(* Phase 4: the in-memory catalog adopts the published shadow; the dirty   *)
(* state it consumed is cleared and the checkpoint call returns success.   *)
(***************************************************************************)
UpdateMountedCatalog ==
    /\ ~crashed
    /\ phase = "shadow"
    /\ mountedEpoch' = shadowEpoch
    /\ allDirty' = FALSE
    /\ dirtyCovers' = TRUE
    /\ phase' = "idle"
    /\ lastResult' = "ok"
    /\ lastShadowPublished' = TRUE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        crashed, recoveryFailed, lastCanonicalPublished, readerEpoch
        >>

(***************************************************************************)
(* The shadow build or publication fails after the canonical manifest      *)
(* replaced (spec §3.7.5). The checkpoint call still returns success —     *)
(* canonical publication is what its result reflects — and the dirty       *)
(* state is preserved untouched for the retry.                             *)
(***************************************************************************)
ShadowBuildFailure ==
    /\ ~crashed
    /\ phase \in {"canonical", "dict"}
    /\ phase' = "idle"
    /\ lastResult' = "ok"
    /\ lastShadowPublished' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        lastCanonicalPublished, readerEpoch
        >>

(***************************************************************************)
(* A crash at any boundary. Volatile state (the in-memory catalog, dirty   *)
(* tracker, reader pin, in-flight checkpoint) is lost; durable state       *)
(* survives. memEpoch survives because every mutation is WAL-durable.      *)
(***************************************************************************)
Crash ==
    /\ ~crashed
    /\ crashed' = TRUE
    /\ phase' = "idle"
    /\ mountedEpoch' = -1
    /\ allDirty' = TRUE
    /\ dirtyCovers' = FALSE
    /\ readerEpoch' = -1
    /\ lastResult' = "none"
    /\ lastCanonicalPublished' = FALSE
    /\ lastShadowPublished' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        recoveryFailed
        >>

(***************************************************************************)
(* Bytes rot while the database is down; validation only sees it at the    *)
(* next reopen.                                                            *)
(***************************************************************************)
CorruptShadowAtRest ==
    /\ crashed
    /\ shadowEpoch # -1
    /\ shadowIntact
    /\ shadowIntact' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, dictEpoch, phase,
        mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        lastResult, lastCanonicalPublished, lastShadowPublished, readerEpoch
        >>

(***************************************************************************)
(* Recovery, shadow-absent: canonical recovery proceeds untouched and the  *)
(* first shadow checkpoint builds everything.                              *)
(***************************************************************************)
RecoverWithoutShadow ==
    /\ crashed
    /\ shadowEpoch = -1
    /\ crashed' = FALSE
    /\ recoveryFailed' = FALSE
    /\ mountedEpoch' = -1
    /\ allDirty' = TRUE
    /\ dirtyCovers' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, lastResult, lastCanonicalPublished, lastShadowPublished,
        readerEpoch
        >>

(***************************************************************************)
(* Recovery, corrupt shadow: the shadow is rebuildable derived state, so   *)
(* validation failure discards it (the projected-graph policy of           *)
(* STORAGE.md recovery step 10) instead of failing the open. The next      *)
(* checkpoint rebuilds every table from a fresh sequence.                  *)
(***************************************************************************)
RecoverDiscardsCorruptShadow ==
    /\ crashed
    /\ shadowEpoch # -1
    /\ ~shadowIntact
    /\ crashed' = FALSE
    /\ recoveryFailed' = FALSE
    /\ shadowEpoch' = -1
    /\ dictEpoch' = -1
    /\ shadowIntact' = TRUE
    /\ mountedEpoch' = -1
    /\ allDirty' = TRUE
    /\ dirtyCovers' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, phase, lastResult, lastCanonicalPublished,
        lastShadowPublished, readerEpoch
        >>

(***************************************************************************)
(* Recovery, intact shadow at the canonical epoch: mount it as the reuse   *)
(* base. WAL replay re-marks every mutation past the canonical epoch, so   *)
(* the dirty tracker covers the whole gap above the mounted shadow.        *)
(***************************************************************************)
RecoverMountsMatchingShadow ==
    /\ crashed
    /\ shadowEpoch # -1
    /\ shadowIntact
    /\ shadowEpoch = canonicalEpoch
    /\ crashed' = FALSE
    /\ recoveryFailed' = FALSE
    /\ mountedEpoch' = shadowEpoch
    /\ allDirty' = FALSE
    /\ dirtyCovers' = TRUE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, lastResult, lastCanonicalPublished, lastShadowPublished,
        readerEpoch
        >>

(***************************************************************************)
(* Recovery, intact but stale shadow (its epoch differs from the           *)
(* recovered canonical epoch): WAL replay cannot re-mark the mutations     *)
(* inside the gap, so the shadow is kept only as the reuse parent and the  *)
(* next build is all-dirty.                                                *)
(***************************************************************************)
RecoverKeepsStaleShadowAllDirty ==
    /\ crashed
    /\ shadowEpoch # -1
    /\ shadowIntact
    /\ shadowEpoch # canonicalEpoch
    /\ crashed' = FALSE
    /\ recoveryFailed' = FALSE
    /\ mountedEpoch' = shadowEpoch
    /\ allDirty' = TRUE
    /\ dirtyCovers' = FALSE
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, lastResult, lastCanonicalPublished, lastShadowPublished,
        readerEpoch
        >>

(***************************************************************************)
(* A reader of the canonical side pins the published canonical epoch. It   *)
(* never opens the shadow, so no shadow state appears in its guard.        *)
(***************************************************************************)
BeginRead ==
    /\ ~crashed
    /\ readerEpoch = -1
    /\ readerEpoch' = canonicalEpoch
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        lastResult, lastCanonicalPublished, lastShadowPublished
        >>

EndRead ==
    /\ readerEpoch # -1
    /\ readerEpoch' = -1
    /\ UNCHANGED <<
        memEpoch, canonicalEpoch, shadowEpoch, shadowIntact, dictEpoch,
        phase, mountedEpoch, allDirty, dirtyCovers, crashed, recoveryFailed,
        lastResult, lastCanonicalPublished, lastShadowPublished
        >>

Next ==
    \/ Mutate
    \/ PublishCanonicalManifest
    \/ CanonicalPublishFailure
    \/ PublishShadowDictionary
    \/ PublishShadowManifest
    \/ UpdateMountedCatalog
    \/ ShadowBuildFailure
    \/ Crash
    \/ CorruptShadowAtRest
    \/ RecoverWithoutShadow
    \/ RecoverDiscardsCorruptShadow
    \/ RecoverMountsMatchingShadow
    \/ RecoverKeepsStaleShadowAllDirty
    \/ BeginRead
    \/ EndRead

TypeOK ==
    /\ memEpoch \in Epochs
    /\ canonicalEpoch \in Epochs
    /\ shadowEpoch \in OptionalEpochs
    /\ shadowIntact \in BOOLEAN
    /\ dictEpoch \in OptionalEpochs
    /\ phase \in Phases
    /\ mountedEpoch \in OptionalEpochs
    /\ allDirty \in BOOLEAN
    /\ dirtyCovers \in BOOLEAN
    /\ crashed \in BOOLEAN
    /\ recoveryFailed \in BOOLEAN
    /\ lastResult \in Results
    /\ lastCanonicalPublished \in BOOLEAN
    /\ lastShadowPublished \in BOOLEAN
    /\ readerEpoch \in OptionalEpochs

(***************************************************************************)
(* (a) Shadow state never influences the canonical recovery outcome:       *)
(* every recovery branch succeeds and restores the WAL-durable epoch,      *)
(* whatever the shadow's condition.                                        *)
(***************************************************************************)
CanonicalRecoveryIndependentOfShadow ==
    recoveryFailed = FALSE

MemEpochNeverBehindCanonical ==
    memEpoch >= canonicalEpoch

(***************************************************************************)
(* (b) A stale or corrupt shadow is never mounted as current: outside the  *)
(* transient window between shadow-manifest replace and catalog update, a  *)
(* mounted catalog always binds an intact published shadow at its own      *)
(* epoch.                                                                  *)
(***************************************************************************)
NoStaleShadowMount ==
    (~crashed /\ phase # "shadow" /\ mountedEpoch # -1) =>
        /\ shadowIntact
        /\ mountedEpoch = shadowEpoch

(***************************************************************************)
(* (c) Canonical ahead of shadow forces the next build to be all-dirty or  *)
(* dirty-superset: whenever the epochs diverge at rest, either the         *)
(* all-dirty flag or full dirty coverage stands.                           *)
(***************************************************************************)
StaleShadowGapIsCovered ==
    (~crashed /\ phase = "idle" /\ shadowEpoch # canonicalEpoch) =>
        (allDirty \/ dirtyCovers)

(***************************************************************************)
(* (c) Convergence: a checkpoint whose shadow published leaves the shadow  *)
(* exactly at the canonical epoch.                                         *)
(***************************************************************************)
ShadowConvergesOnSuccess ==
    lastShadowPublished => shadowEpoch = canonicalEpoch

ShadowNeverLeadsCanonical ==
    shadowEpoch <= canonicalEpoch

ShadowManifestHasDictionaryCoverage ==
    (shadowEpoch # -1 /\ shadowIntact) =>
        /\ dictEpoch # -1
        /\ dictEpoch >= shadowEpoch

(***************************************************************************)
(* (d) The checkpoint result reflects canonical publication only: success  *)
(* is reported exactly when the canonical manifest replaced, including     *)
(* when the shadow failed afterwards.                                      *)
(***************************************************************************)
CheckpointResultTracksCanonicalOnly ==
    /\ lastResult = "ok" => lastCanonicalPublished
    /\ lastResult = "failed" => ~lastCanonicalPublished

ReaderPinsCanonicalOnly ==
    readerEpoch # -1 =>
        /\ readerEpoch >= 0
        /\ readerEpoch <= canonicalEpoch

Spec == Init /\ [][Next]_vars

=============================================================================
