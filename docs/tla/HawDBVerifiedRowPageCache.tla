---------------------- MODULE HawDBVerifiedRowPageCache ----------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A relational row page is persisted in one fixed physical slot but only  *)
(* its encoded prefix is retained in the shared cache. A cold read validates *)
(* the complete slot once, including the zero tail and strong source tag,    *)
(* before publishing the compact cache entry. A warm read can skip that      *)
(* validation only when the exact source tag remains attached to the entry.  *)
(* Untagged cache lookup cannot observe a verified compact entry.             *)
(***************************************************************************)

CONSTANT SlotCount, SlotBytes

ASSUME /\ SlotCount \in Nat \ {0}
       /\ SlotBytes \in Nat \ {0}
       /\ SlotCount < SlotBytes

Slots == 1..SlotCount
Phases == {"idle", "reading", "done"}
Outcomes == {"none", "success", "corruption"}
ReadPaths == {"none", "verified-hit", "validated-slot"}

SourceTag(slot) == slot
EncodedLen(slot) == slot

VARIABLES
    phase,
    outcome,
    readPath,
    requestSlot,
    requestValidations,
    sourceCorrupt,
    cacheSlot,
    cacheTag,
    cacheBytes,
    poisoned

vars == <<
    phase,
    outcome,
    readPath,
    requestSlot,
    requestValidations,
    sourceCorrupt,
    cacheSlot,
    cacheTag,
    cacheBytes,
    poisoned
>>

Init ==
    /\ phase = "idle"
    /\ outcome = "none"
    /\ readPath = "none"
    /\ requestSlot = 0
    /\ requestValidations = 0
    /\ sourceCorrupt \in SUBSET Slots
    /\ cacheSlot = 0
    /\ cacheTag = 0
    /\ cacheBytes = 0
    /\ poisoned = FALSE

BeginRead ==
    /\ phase = "idle"
    /\ ~poisoned
    /\ \E slot \in Slots: requestSlot' = slot
    /\ phase' = "reading"
    /\ outcome' = "none"
    /\ readPath' = "none"
    /\ requestValidations' = 0
    /\ UNCHANGED <<
        sourceCorrupt,
        cacheSlot,
        cacheTag,
        cacheBytes,
        poisoned
        >>

VerifiedHit ==
    /\ phase = "reading"
    /\ cacheSlot = requestSlot
    /\ cacheTag = SourceTag(requestSlot)
    /\ cacheBytes = EncodedLen(requestSlot)
    /\ phase' = "done"
    /\ outcome' = "success"
    /\ readPath' = "verified-hit"
    /\ UNCHANGED <<
        requestSlot,
        requestValidations,
        sourceCorrupt,
        cacheSlot,
        cacheTag,
        cacheBytes,
        poisoned
        >>

ValidateHealthySlot ==
    /\ phase = "reading"
    /\ \/ cacheSlot # requestSlot
       \/ cacheTag # SourceTag(requestSlot)
    /\ requestSlot \notin sourceCorrupt
    /\ phase' = "done"
    /\ outcome' = "success"
    /\ readPath' = "validated-slot"
    /\ requestValidations' = 1
    /\ cacheSlot' = requestSlot
    /\ cacheTag' = SourceTag(requestSlot)
    /\ cacheBytes' = EncodedLen(requestSlot)
    /\ UNCHANGED <<requestSlot, sourceCorrupt, poisoned>>

ValidateCorruptSlot ==
    /\ phase = "reading"
    /\ \/ cacheSlot # requestSlot
       \/ cacheTag # SourceTag(requestSlot)
    /\ requestSlot \in sourceCorrupt
    /\ phase' = "done"
    /\ outcome' = "corruption"
    /\ readPath' = "validated-slot"
    /\ requestValidations' = 1
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        requestSlot,
        sourceCorrupt,
        cacheSlot,
        cacheTag,
        cacheBytes
        >>

FinishRead ==
    /\ phase = "done"
    /\ outcome = "success"
    /\ phase' = "idle"
    /\ outcome' = "none"
    /\ readPath' = "none"
    /\ requestSlot' = 0
    /\ requestValidations' = 0
    /\ UNCHANGED <<
        sourceCorrupt,
        cacheSlot,
        cacheTag,
        cacheBytes,
        poisoned
        >>

EvictCompactEntry ==
    /\ phase = "idle"
    /\ cacheSlot # 0
    /\ cacheSlot' = 0
    /\ cacheTag' = 0
    /\ cacheBytes' = 0
    /\ UNCHANGED <<
        phase,
        outcome,
        readPath,
        requestSlot,
        requestValidations,
        sourceCorrupt,
        poisoned
        >>

Next ==
    \/ BeginRead
    \/ VerifiedHit
    \/ ValidateHealthySlot
    \/ ValidateCorruptSlot
    \/ FinishRead
    \/ EvictCompactEntry

TypeOK ==
    /\ phase \in Phases
    /\ outcome \in Outcomes
    /\ readPath \in ReadPaths
    /\ requestSlot \in Slots \cup {0}
    /\ requestValidations \in 0..1
    /\ sourceCorrupt \subseteq Slots
    /\ cacheSlot \in Slots \cup {0}
    /\ cacheTag \in Slots \cup {0}
    /\ cacheBytes \in 0..SlotBytes
    /\ poisoned \in BOOLEAN

VerifiedEntryBindsExactSource ==
    cacheSlot # 0 =>
        /\ cacheTag = SourceTag(cacheSlot)
        /\ cacheSlot \notin sourceCorrupt

VerifiedEntryIsCompact ==
    /\ cacheSlot = 0 => cacheBytes = 0
    /\ cacheSlot # 0 =>
        /\ cacheBytes = EncodedLen(cacheSlot)
        /\ cacheBytes < SlotBytes

UntaggedLookupCannotSeeVerifiedEntry ==
    cacheSlot # 0 => cacheTag # 0

AtMostOneStrongValidationPerRead == requestValidations <= 1

VerifiedHitSkipsStrongValidation ==
    phase = "done" /\ readPath = "verified-hit" => requestValidations = 0

ColdReadValidatesExactlyOnce ==
    phase = "done" /\ readPath = "validated-slot" => requestValidations = 1

SuccessUsesAuthenticatedBytes ==
    outcome = "success" => readPath \in {"verified-hit", "validated-slot"}

OnlyCorruptionPoisons == poisoned <=> outcome = "corruption"

Spec == Init /\ [][Next]_vars

=============================================================================
