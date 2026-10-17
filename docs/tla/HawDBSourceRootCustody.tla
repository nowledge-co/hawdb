---------------- MODULE HawDBSourceRootCustody ----------------
EXTENDS Integers, FiniteSets
CONSTANTS DropOnParent, ReleaseDuringRead, IgnoreOncePins, IgnoreRetryPins,
 PathForUnlink, PathForRead, RetainBusy, PinWholeManifest, UnlockBeforePin
VARIABLES head, phase, owners, pins, reading, readRoot, available,
 namespace, busy, pending, unlinkRoot
vars == <<head, phase, owners, pins, reading, readRoot, available,
 namespace, busy, pending, unlinkRoot>>
Roots == {1, 2}
Artifacts == {0, 1, 2}
OwnerNames == {"parent", "child", "read"}
Init == /\ head=1 /\ phase="unopened" /\ owners={} /\ pins={}
 /\ reading=FALSE /\ readRoot=1 /\ available=[r \in Roots |-> Artifacts]
 /\ namespace=1 /\ busy="idle" /\ pending={} /\ unlinkRoot=1
OpenBegin == /\ phase="unopened" /\ head=1 /\ busy="idle"
 /\ phase'="opening" /\ busy'=(IF UnlockBeforePin THEN "idle" ELSE "reader")
 /\ UNCHANGED <<head,owners,pins,reading,readRoot,available,namespace,pending,unlinkRoot>>
InstallPin == /\ phase="opening" /\ busy \in {"reader","idle"}
 /\ phase'="ready" /\ owners'={"parent"}
 /\ pins'=(IF PinWholeManifest THEN {0,1} ELSE {0})
 /\ busy'=(IF RetainBusy THEN "reader" ELSE "idle")
 /\ UNCHANGED <<head,reading,readRoot,available,namespace,pending,unlinkRoot>>
CloneChild == /\ phase="ready" /\ "parent" \in owners /\ "child" \notin owners
 /\ owners'=owners \cup {"child"}
 /\ UNCHANGED <<head,phase,pins,reading,readRoot,available,namespace,busy,pending,unlinkRoot>>
DropParent == /\ "parent" \in owners
 /\ owners'=owners \ {"parent"}
 /\ pins'=(IF DropOnParent \/ owners'={} THEN {} ELSE pins)
 /\ UNCHANGED <<head,phase,reading,readRoot,available,namespace,busy,pending,unlinkRoot>>
BeginRead == /\ "child" \in owners /\ ~reading
 /\ owners'=owners \cup {"read"} /\ reading'=TRUE
 /\ readRoot'=(IF PathForRead THEN namespace ELSE 1)
 /\ UNCHANGED <<head,phase,pins,available,namespace,busy,pending,unlinkRoot>>
DropChild == /\ "child" \in owners
 /\ owners'=owners \ {"child"}
 /\ pins'=(IF owners'={} \/ (ReleaseDuringRead /\ reading) THEN {} ELSE pins)
 /\ UNCHANGED <<head,phase,reading,readRoot,available,namespace,busy,pending,unlinkRoot>>
EndRead == /\ reading /\ owners'=owners \ {"read"} /\ reading'=FALSE
 /\ pins'=(IF owners'={} THEN {} ELSE pins)
 /\ UNCHANGED <<head,phase,readRoot,available,namespace,busy,pending,unlinkRoot>>
Publish == /\ busy="idle" /\ head=1 /\ head'=2
 /\ UNCHANGED <<phase,owners,pins,reading,readRoot,available,namespace,busy,pending,unlinkRoot>>
ReplaceNamespace == /\ namespace=1 /\ namespace'=2
 /\ UNCHANGED <<head,phase,owners,pins,reading,readRoot,available,busy,pending,unlinkRoot>>
SelectCleanup(kind) == /\ busy="idle" /\ head=2 /\ kind \in {"once","retry"}
 /\ busy'="cleanup"
 /\ pending'=(IF (kind="once" /\ IgnoreOncePins) \/ (kind="retry" /\ IgnoreRetryPins)
              THEN {0} \cap available[1] ELSE ({0} \cap available[1]) \ pins)
 /\ unlinkRoot'=(IF PathForUnlink THEN namespace ELSE 1)
 /\ UNCHANGED <<head,phase,owners,pins,reading,readRoot,available,namespace>>
Unlink == /\ busy="cleanup"
 /\ available'=[available EXCEPT ![unlinkRoot]=@ \ pending]
 /\ pending'={} /\ busy'="idle"
 /\ UNCHANGED <<head,phase,owners,pins,reading,readRoot,namespace,unlinkRoot>>
Next == OpenBegin \/ InstallPin \/ CloneChild \/ DropParent \/ BeginRead \/ DropChild \/ EndRead
 \/ Publish \/ ReplaceNamespace \/ (\E kind \in {"once","retry"}:SelectCleanup(kind)) \/ Unlink
TypeOK == /\ head \in {1,2} /\ phase \in {"unopened","opening","ready"}
 /\ owners \subseteq OwnerNames /\ pins \subseteq {0,1}
 /\ reading \in BOOLEAN /\ (reading <=> "read" \in owners)
 /\ readRoot \in Roots /\ available \in [Roots -> SUBSET Artifacts]
 /\ namespace \in Roots /\ busy \in {"idle","reader","cleanup"}
 /\ pending \subseteq {0} /\ unlinkRoot \in Roots
SourcesLive == owners#{} => 0 \in available[1]
ForeignRootUntouched == available[2]=Artifacts
ReadUsesCapturedRoot == reading => readRoot=1
ReaderDoesNotExcludeWriter == phase="ready" => busy#"reader"
ChildPinIsExact == owners#{} /\ "parent" \notin owners => pins \subseteq {0}
Spec == Init /\ [][Next]_vars
=============================================================================
