---------------- MODULE HawDBSearchCleanupPublication ----------------
EXTENDS Integers, FiniteSets
CONSTANTS SkipExclusion, UseStaleHint, ForgetReferences, ReleaseEarly, IgnoreUnknownClosure
VARIABLES head, refs, files, memoryVectors, owner, candidate, vector,
 installed, synced, committed, deferred, observed, decisionRefs, removeAll,
 known, uncertainFiles, standalone, loadedHead, loadedQ, residentMode, decisionTrusted
vars == <<head,refs,files,memoryVectors,owner,candidate,vector,
 installed,synced,committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Generations == 1..3
KnownRefs == IF known THEN refs ELSE {}
Init == /\ head=1 /\ refs={} /\ files={} /\ memoryVectors=FALSE
 /\ owner="none" /\ candidate=0 /\ vector=FALSE
 /\ installed=FALSE /\ synced=FALSE /\ committed=FALSE
 /\ deferred=FALSE /\ observed=FALSE /\ decisionRefs={} /\ removeAll=FALSE
 /\ known=TRUE /\ uncertainFiles={}
 /\ standalone={} /\ loadedHead=1 /\ loadedQ=0
 /\ residentMode=FALSE /\ decisionTrusted=TRUE
Prepare == /\ known /\ owner="none" /\ candidate=0 /\ head<3
 /\ owner'="writer" /\ candidate'=head+1 /\ vector' \in BOOLEAN /\ residentMode' \in BOOLEAN
 /\ installed'=FALSE /\ synced'=FALSE /\ committed'=FALSE
 /\ UNCHANGED <<head,refs,files,memoryVectors,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,loadedHead,loadedQ,decisionTrusted>>
Install == /\ owner="writer" /\ ~installed
 /\ files'=(IF vector THEN files \cup {candidate} ELSE files)
 /\ uncertainFiles'=(IF ~known /\ vector THEN uncertainFiles \cup {candidate} ELSE uncertainFiles)
 /\ installed'=TRUE
 /\ UNCHANGED <<head,refs,memoryVectors,owner,candidate,vector,synced,
                 committed,deferred,observed,decisionRefs,removeAll,known,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Sync == /\ owner="writer" /\ installed /\ ~synced /\ synced'=TRUE
 /\ UNCHANGED <<head,refs,files,memoryVectors,owner,candidate,vector,
                 installed,committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Publish == /\ known /\ owner="writer" /\ synced /\ ~committed
 /\ head'=candidate
 /\ refs'=(IF vector /\ ~residentMode THEN {candidate} ELSE {})
 /\ standalone'=(IF vector /\ residentMode THEN {candidate} ELSE {})
 /\ committed'=TRUE
 /\ UNCHANGED <<files,memoryVectors,owner,candidate,vector,installed,synced,
                 deferred,observed,decisionRefs,removeAll,known,uncertainFiles,loadedHead,loadedQ,residentMode,decisionTrusted>>
Release == /\ owner="writer" /\ (committed \/ ReleaseEarly)
 /\ owner'="none"
 /\ candidate'=(IF committed THEN 0 ELSE candidate)
 /\ UNCHANGED <<head,refs,files,memoryVectors,vector,installed,synced,
                 committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Cancel == /\ owner="writer" /\ ~committed
 /\ owner'="none" /\ candidate'=0
 /\ UNCHANGED <<head,refs,files,memoryVectors,vector,installed,synced,
                 committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Refresh == /\ known /\ memoryVectors'=(refs \cup standalone#{})
 /\ loadedHead'=head
 /\ loadedQ'=(IF refs \cup standalone={} THEN 0 ELSE CHOOSE g \in refs \cup standalone: TRUE)
 /\ UNCHANGED <<head,refs,files,owner,candidate,vector,installed,synced,
                 committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,standalone,residentMode,decisionTrusted>>
Damage == /\ known /\ known'=FALSE /\ uncertainFiles'=files
 /\ UNCHANGED <<head,refs,files,memoryVectors,owner,candidate,vector,
 installed,synced,committed,deferred,observed,decisionRefs,removeAll,standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Remove(hint, current, protected) ==
 IF ~known /\ ~IgnoreUnknownClosure THEN {}
 ELSE IF hint THEN (IF ForgetReferences THEN files ELSE files\protected)
 ELSE {g \in files: current#0 /\ g<current-1 /\ g \notin protected}
PublicCleanup ==
 /\ (IF owner#"none" /\ ~SkipExclusion
      THEN /\ deferred'=TRUE
           /\ UNCHANGED <<files,observed,decisionRefs,removeAll,decisionTrusted>>
      ELSE IF ~known /\ ~IgnoreUnknownClosure
      THEN /\ deferred'=TRUE /\ observed'=FALSE /\ decisionRefs'={}
           /\ removeAll'=FALSE /\ decisionTrusted'=FALSE /\ UNCHANGED files
      ELSE /\ LET trusted == loadedHead=head
                   hint == IF UseStaleHint THEN ~memoryVectors
                           ELSE KnownRefs={} /\ trusted /\ ~memoryVectors
                   current == IF KnownRefs#{} THEN CHOOSE g \in KnownRefs: TRUE
                              ELSE IF trusted THEN loadedQ ELSE 0
              IN /\ files'=files\Remove(hint,current,KnownRefs)
                 /\ removeAll'=hint /\ decisionTrusted'=trusted
           /\ observed'=known /\ decisionRefs'=KnownRefs /\ UNCHANGED deferred)
 /\ UNCHANGED <<head,refs,memoryVectors,owner,candidate,vector,installed,synced,
                 committed,known,uncertainFiles,standalone,loadedHead,loadedQ,residentMode>>
\* The raw sweep caller supplies its complete retained set. Standalone
\* resident authority is represented here by an explicit retained reference.
RawCleanup == /\ owner="none"
 /\ \E hint \in BOOLEAN:
      files'=files\Remove(hint,head,KnownRefs \cup standalone)
 /\ UNCHANGED <<head,refs,memoryVectors,owner,candidate,vector,installed,synced,
                 committed,deferred,observed,decisionRefs,removeAll,known,uncertainFiles,
                 standalone,loadedHead,loadedQ,residentMode,decisionTrusted>>
Next == Prepare \/ Install \/ Sync \/ Publish \/ Release \/ Cancel \/ Refresh \/ Damage \/ PublicCleanup \/ RawCleanup
TypeOK == /\ head \in Generations /\ refs \subseteq Generations /\ files \subseteq Generations
 /\ owner \in {"none","writer"} /\ candidate \in 0..3
 /\ vector \in BOOLEAN /\ memoryVectors \in BOOLEAN /\ installed \in BOOLEAN
 /\ synced \in BOOLEAN /\ committed \in BOOLEAN /\ deferred \in BOOLEAN
 /\ observed \in BOOLEAN /\ decisionRefs \subseteq Generations /\ removeAll \in BOOLEAN
 /\ known \in BOOLEAN /\ uncertainFiles \subseteq Generations
 /\ standalone \subseteq Generations /\ loadedHead \in Generations
 /\ loadedQ \in 0..3 /\ residentMode \in BOOLEAN /\ decisionTrusted \in BOOLEAN
CurrentSurvives == refs \cup standalone \subseteq files
PendingSurvives == candidate#0 /\ vector /\ installed /\ ~committed => candidate \in files
CurrentAuthority == observed /\ removeAll => decisionRefs={} /\ decisionTrusted
UncertainRetains == ~known => uncertainFiles \subseteq files
NoStaleSafeCleanupWitness == ~(head>1 /\ refs#{} /\ ~memoryVectors
 /\ observed /\ decisionRefs=refs /\ refs \subseteq files)
NoStandaloneWitness == ~(standalone#{} /\ loadedHead#head /\ observed
 /\ standalone \subseteq files)
NoDeferralWitness == ~(known /\ owner="writer" /\ deferred)
=============================================================================
