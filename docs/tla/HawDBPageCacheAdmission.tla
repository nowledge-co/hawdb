---------------------- MODULE HawDBPageCacheAdmission ----------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* The cache capacity is a caller-carved domain below the root memory       *)
(* budget. Immutable relational index pages are admitted by physical        *)
(* generation/page/digest/representation identity. Foreground reads may     *)
(* bypass a full pinned cache, cancellation releases pins, and background   *)
(* population always completes by admission, eviction, a hit, or bypass.    *)
(***************************************************************************)

CONSTANT PageCount, GenerationCount, CacheCapacity, ForegroundReserve,
         RootCapacity

ASSUME /\ PageCount \in Nat \ {0}
       /\ GenerationCount \in Nat \ {0}
       /\ CacheCapacity \in Nat \ {0}
       /\ ForegroundReserve \in Nat \ {0}
       /\ RootCapacity \in Nat \ {0}
       /\ CacheCapacity + ForegroundReserve <= RootCapacity

Pages == 1..PageCount
Generations == 1..GenerationCount
Representations == {"relational-index-page"}

Identities ==
    { [generation |-> generation,
       page |-> page,
       digest |-> generation * (PageCount + 1) + page,
       representation |-> "relational-index-page"] :
        generation \in Generations, page \in Pages }

NoIdentity == [generation |-> 0, page |-> 0, digest |-> 0,
               representation |-> "none"]
ForegroundStates == {"idle", "using"}
Phases == {"closed", "open"}

VARIABLES
    phase,
    resident,
    pinned,
    referenced,
    foregroundState,
    foregroundIdentity,
    foregroundCached,
    pendingBackground,
    completedBackground,
    poisoned,
    corruptIdentity,
    openResidentCount

vars == <<
    phase,
    resident,
    pinned,
    referenced,
    foregroundState,
    foregroundIdentity,
    foregroundCached,
    pendingBackground,
    completedBackground,
    poisoned,
    corruptIdentity,
    openResidentCount
>>

Init ==
    /\ phase = "closed"
    /\ resident = {}
    /\ pinned = {}
    /\ referenced = {}
    /\ foregroundState = "idle"
    /\ foregroundIdentity = NoIdentity
    /\ foregroundCached = FALSE
    /\ pendingBackground = FALSE
    /\ completedBackground = FALSE
    /\ poisoned = FALSE
    /\ corruptIdentity = NoIdentity
    /\ openResidentCount = 0

Open ==
    /\ phase = "closed"
    /\ phase' = "open"
    /\ openResidentCount' = Cardinality(resident)
    /\ UNCHANGED <<
        resident, pinned, referenced, foregroundState, foregroundIdentity,
        foregroundCached, pendingBackground, completedBackground, poisoned,
        corruptIdentity
        >>

ForegroundHit ==
    /\ phase = "open"
    /\ foregroundState = "idle"
    /\ ~poisoned
    /\ \E identity \in resident:
        /\ foregroundIdentity' = identity
        /\ pinned' = pinned \cup {identity}
        /\ referenced' = referenced \cup {identity}
    /\ foregroundState' = "using"
    /\ foregroundCached' = TRUE
    /\ UNCHANGED <<
        phase, resident, pendingBackground, completedBackground, poisoned,
        corruptIdentity, openResidentCount
        >>

ForegroundAdmit ==
    /\ phase = "open"
    /\ foregroundState = "idle"
    /\ ~poisoned
    /\ Cardinality(resident) < CacheCapacity
    /\ \E identity \in Identities \ resident:
        /\ resident' = resident \cup {identity}
        /\ pinned' = pinned \cup {identity}
        /\ referenced' = referenced \cup {identity}
        /\ foregroundIdentity' = identity
    /\ foregroundState' = "using"
    /\ foregroundCached' = TRUE
    /\ UNCHANGED <<
        phase, pendingBackground, completedBackground, poisoned,
        corruptIdentity, openResidentCount
        >>

ForegroundEvictAndAdmit ==
    /\ phase = "open"
    /\ foregroundState = "idle"
    /\ ~poisoned
    /\ Cardinality(resident) = CacheCapacity
    /\ \E victim \in resident \ pinned:
        \E identity \in Identities \ resident:
            /\ resident' = (resident \ {victim}) \cup {identity}
            /\ pinned' = pinned \cup {identity}
            /\ referenced' = (referenced \ {victim}) \cup {identity}
            /\ foregroundIdentity' = identity
    /\ foregroundState' = "using"
    /\ foregroundCached' = TRUE
    /\ UNCHANGED <<
        phase, pendingBackground, completedBackground, poisoned,
        corruptIdentity, openResidentCount
        >>

ForegroundBypass ==
    /\ phase = "open"
    /\ foregroundState = "idle"
    /\ ~poisoned
    /\ Cardinality(resident) = CacheCapacity
    /\ resident = pinned
    /\ \E identity \in Identities \ resident:
        foregroundIdentity' = identity
    /\ foregroundState' = "using"
    /\ foregroundCached' = FALSE
    /\ UNCHANGED <<
        phase, resident, pinned, referenced, pendingBackground,
        completedBackground, poisoned, corruptIdentity, openResidentCount
        >>

ReleaseForeground ==
    /\ phase = "open"
    /\ foregroundState = "using"
    /\ pinned' = IF foregroundCached
                  THEN pinned \ {foregroundIdentity}
                  ELSE pinned
    /\ foregroundState' = "idle"
    /\ foregroundIdentity' = NoIdentity
    /\ foregroundCached' = FALSE
    /\ UNCHANGED <<
        phase, resident, referenced, pendingBackground, completedBackground,
        poisoned, corruptIdentity, openResidentCount
        >>

CancelForeground == ReleaseForeground

RequestBackground ==
    /\ phase = "open"
    /\ ~pendingBackground
    /\ ~completedBackground
    /\ pendingBackground' = TRUE
    /\ UNCHANGED <<
        phase, resident, pinned, referenced, foregroundState,
        foregroundIdentity, foregroundCached, completedBackground, poisoned,
        corruptIdentity, openResidentCount
        >>

BackgroundHit ==
    /\ pendingBackground
    /\ ~poisoned
    /\ resident # {}
    /\ pendingBackground' = FALSE
    /\ completedBackground' = TRUE
    /\ referenced' = resident
    /\ UNCHANGED <<
        phase, resident, pinned, foregroundState, foregroundIdentity,
        foregroundCached, poisoned, corruptIdentity, openResidentCount
        >>

BackgroundAdmit ==
    /\ pendingBackground
    /\ ~poisoned
    /\ Cardinality(resident) < CacheCapacity
    /\ \E identity \in Identities \ resident:
        /\ resident' = resident \cup {identity}
        /\ referenced' = referenced \cup {identity}
    /\ pendingBackground' = FALSE
    /\ completedBackground' = TRUE
    /\ UNCHANGED <<
        phase, pinned, foregroundState, foregroundIdentity,
        foregroundCached, poisoned, corruptIdentity, openResidentCount
        >>

BackgroundEvictAndAdmit ==
    /\ pendingBackground
    /\ ~poisoned
    /\ Cardinality(resident) = CacheCapacity
    /\ \E victim \in resident \ pinned:
        \E identity \in Identities \ resident:
            /\ resident' = (resident \ {victim}) \cup {identity}
            /\ referenced' = (referenced \ {victim}) \cup {identity}
    /\ pendingBackground' = FALSE
    /\ completedBackground' = TRUE
    /\ UNCHANGED <<
        phase, pinned, foregroundState, foregroundIdentity,
        foregroundCached, poisoned, corruptIdentity, openResidentCount
        >>

BackgroundBypass ==
    /\ pendingBackground
    /\ \/ poisoned
       \/ /\ Cardinality(resident) = CacheCapacity
          /\ resident = pinned
    /\ pendingBackground' = FALSE
    /\ completedBackground' = TRUE
    /\ UNCHANGED <<
        phase, resident, pinned, referenced, foregroundState,
        foregroundIdentity, foregroundCached, poisoned, corruptIdentity,
        openResidentCount
        >>

CompleteBackground ==
    \/ BackgroundHit
    \/ BackgroundAdmit
    \/ BackgroundEvictAndAdmit
    \/ BackgroundBypass

ReadCorruptPage ==
    /\ phase = "open"
    /\ ~poisoned
    /\ \E identity \in Identities \ resident:
        corruptIdentity' = identity
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        phase, resident, pinned, referenced, foregroundState,
        foregroundIdentity, foregroundCached, pendingBackground,
        completedBackground, openResidentCount
        >>

Next ==
    \/ Open
    \/ ForegroundHit
    \/ ForegroundAdmit
    \/ ForegroundEvictAndAdmit
    \/ ForegroundBypass
    \/ ReleaseForeground
    \/ CancelForeground
    \/ RequestBackground
    \/ CompleteBackground
    \/ ReadCorruptPage

TypeOK ==
    /\ phase \in Phases
    /\ resident \subseteq Identities
    /\ pinned \subseteq Identities
    /\ referenced \subseteq Identities
    /\ foregroundState \in ForegroundStates
    /\ foregroundIdentity \in Identities \cup {NoIdentity}
    /\ foregroundCached \in BOOLEAN
    /\ pendingBackground \in BOOLEAN
    /\ completedBackground \in BOOLEAN
    /\ poisoned \in BOOLEAN
    /\ corruptIdentity \in Identities \cup {NoIdentity}
    /\ openResidentCount \in Nat

ResidentWithinCapacity == Cardinality(resident) <= CacheCapacity

PinsRemainResident == pinned \subseteq resident

ReferencesRemainResident == referenced \subseteq resident

ForegroundPinIsSafe ==
    foregroundState = "using" /\ foregroundCached =>
        foregroundIdentity \in pinned

CacheDomainPreservesForegroundReserve ==
    Cardinality(resident) + ForegroundReserve <= RootCapacity

OpenDoesNotWarmPages == openResidentCount = 0

CorruptPageIsNeverAdmitted ==
    corruptIdentity # NoIdentity => corruptIdentity \notin resident

BackgroundTerminates == pendingBackground ~> ~pendingBackground

Spec == Init /\ [][Next]_vars /\ WF_vars(CompleteBackground)

=============================================================================
