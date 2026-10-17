---------------- MODULE HawDBConsumerMutationCustody ----------------
EXTENDS Integers, FiniteSets
CONSTANTS IgnoreGeneration, AdmitAfterClose, ReleaseBusyEarly, ForgetNativePin,
          SkipOwnerAssociation
(* One fixed native owner identity, two operation generations and two admitted
   methods. Move changes the index occupying the owner's slot; the foreign
   index can carry the SAME numeric generation from a different authority.
   Atomic transitions abstract the single-word CAS, not filesystem durability. *)
VARIABLE s
vars == <<s>>
Slots == 1..2
Init == s = [generation |-> 0, scope |-> FALSE, open |-> FALSE,
 refs |-> 0, owner |-> TRUE, peer |-> FALSE, stamp |-> 0,
 localIndex |-> TRUE, guards |-> [i \in Slots |-> 0], badAdmission |-> FALSE,
 badOwnerUse |-> FALSE]
LiveGuards == {i \in Slots: s.guards[i] # 0}
Begin == /\ s.owner /\ ~s.scope /\ s.generation < 2
 /\ (IF ReleaseBusyEarly THEN TRUE ELSE s.refs = 0)
 /\ s' = [s EXCEPT !.generation = @+1, !.scope = TRUE, !.open = FALSE,
                    !.refs = @+1]
Open == /\ s.scope /\ ~s.open
 /\ s' = [s EXCEPT !.open = TRUE]
Stamp == /\ s.open /\ s' = [s EXCEPT !.stamp = s.generation]
Move == /\ s' = [s EXCEPT !.localIndex = ~@]
Close == /\ s.scope
 /\ s' = [s EXCEPT !.scope = FALSE, !.open = FALSE, !.refs = @-1]
Admit(i) == /\ i \in Slots /\ s.guards[i] = 0 /\ s.stamp # 0
 /\ (s.open \/ AdmitAfterClose)
 /\ (s.stamp = s.generation \/ IgnoreGeneration)
 /\ (s.localIndex \/ SkipOwnerAssociation)
 /\ (s.owner \/ s.scope \/ LiveGuards # {})
 /\ s' = [s EXCEPT !.guards[i] = s.generation, !.refs = @+1,
      !.badAdmission = @ \/ ~s.open \/ s.stamp # s.generation \/ ~s.localIndex]
Finish(i) == /\ i \in LiveGuards
 /\ s' = [s EXCEPT !.guards[i] = 0, !.refs = @-1]
OwnerUse == /\ s.owner /\ (s.localIndex \/ SkipOwnerAssociation)
 /\ s' = [s EXCEPT !.badOwnerUse = @ \/ ~s.localIndex]
DropOwner == /\ s.owner /\ s' = [s EXCEPT !.owner = FALSE]
PeerAcquire == /\ ~s.owner /\ ~s.scope
 /\ (LiveGuards = {} \/ ForgetNativePin)
 /\ s' = [s EXCEPT !.peer = TRUE]
Next == Begin \/ Open \/ Stamp \/ Move \/ Close \/ OwnerUse \/ DropOwner \/ PeerAcquire
     \/ (\E i \in Slots: Admit(i) \/ Finish(i))
TypeOK == /\ s.generation \in 0..2 /\ s.stamp \in 0..2 /\ s.refs \in 0..3
 /\ s.guards \in [Slots -> 0..2]
 /\ \A k \in {"scope", "open", "owner", "peer", "localIndex",
                "badAdmission", "badOwnerUse"}: s[k] \in BOOLEAN
ReferenceConservation == s.refs = Cardinality(LiveGuards) + (IF s.scope THEN 1 ELSE 0)
AdmissionRevoked == ~s.badAdmission
OwnerAssociation == ~s.badOwnerUse
NoEpochOverlap == \A i \in LiveGuards: s.guards[i] = s.generation
NativeExclusion == s.peer => ~s.scope /\ LiveGuards = {}
OpenNeedsScope == s.open => s.scope
=============================================================================
