---------------- MODULE HawDBAdmittedNativeLease ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Clients, EvictBusy, PublishEarly, SkipCharge, KeepReturnedPin
ASSUME /\ IsFiniteSet(Clients) /\ Clients # {} /\ 0 \notin Clients
       /\ EvictBusy \in BOOLEAN /\ PublishEarly \in BOOLEAN
       /\ SkipCharge \in BOOLEAN /\ KeepReturnedPin \in BOOLEAN

(* One immutable object and one cache-opening lock. Logical pinned copies and
   scoped reads share a native File/descriptor permit. Validation has two symbolic
   chunks. The model assumes immutable object identity, atomic counted Arc
   references and managed FD admission; it does not prove SHA, Arc, byte bounds,
   cache heap accounting, GC root authority, fairness or MVCC activation. *)
VARIABLE s
vars == <<s>>
Init == s = [native |-> 0, charged |-> 0, cache |-> FALSE,
 pins |-> {}, reads |-> {}, owner |-> 0, admitted |-> FALSE, chunks |-> 0,
 validated |-> FALSE, verified |-> {}, returned |-> {}, stopped |-> {}]

ColdOpen(c) == /\ s.native=0 /\ s.owner=0
 /\ c \notin s.returned \cup s.stopped
 /\ s'=[s EXCEPT !.native=1, !.charged=IF SkipCharge THEN 0 ELSE 1,
                   !.owner=c, !.admitted=FALSE, !.chunks=0]
AdmitValidation(c) == /\ s.owner=c /\ ~s.admitted /\ c \notin s.stopped
 /\ s'=[s EXCEPT !.admitted=TRUE]
ValidationChunk(c) == /\ s.owner=c /\ s.admitted /\ s.chunks<2
 /\ c \notin s.stopped
 /\ s'=[s EXCEPT !.chunks=@+1]
PublishValidated(c) == /\ s.owner=c /\ c \notin s.stopped
 /\ ((s.admitted /\ s.chunks=2) \/ PublishEarly)
 /\ s'=[s EXCEPT !.owner=0, !.cache=TRUE,
         !.validated=s.admitted /\ s.chunks=2,
         !.verified=IF s.admitted /\ s.chunks=2 THEN @ \cup {c} ELSE @,
         !.pins=@ \cup {c}]
AbandonValidation(c) == /\ s.owner=c /\ c \in s.stopped
 /\ s'=[s EXCEPT !.owner=0, !.native=0, !.charged=0,
                   !.admitted=FALSE, !.chunks=0]
WarmPin(c) == /\ s.cache /\ s.validated /\ s.owner=0
 /\ c \notin s.pins \cup s.returned \cup s.stopped
 /\ s'=[s EXCEPT !.pins=@ \cup {c}, !.verified=@ \cup {c}]
ClonePin(c,d) == /\ c \in s.pins
 /\ d \notin s.pins \cup s.returned \cup s.stopped
 /\ s'=[s EXCEPT !.pins=@ \cup {d}, !.verified=@ \cup {d}]
BeginRead(c) == /\ c \in s.pins /\ c \notin s.reads \cup s.stopped
 /\ s'=[s EXCEPT !.reads=@ \cup {c}]
EndRead(c) == /\ c \in s.reads
 /\ s'=[s EXCEPT !.reads=@ \ {c}]
DropPin(c) == /\ c \in s.pins
 /\ s'=[s EXCEPT !.pins=@ \ {c}]
ReturnMapped(c) == /\ c \in s.pins /\ c \notin s.reads \cup s.stopped
 /\ s'=[s EXCEPT !.pins=IF KeepReturnedPin THEN @ ELSE @ \ {c},
                   !.returned=@ \cup {c}]
Stop(c) == /\ c \notin s.stopped
 /\ s'=[s EXCEPT !.stopped=@ \cup {c}]
Evict == /\ s.cache /\ s.owner=0
 /\ ((s.pins={} /\ s.reads={}) \/ EvictBusy)
 /\ s'=[s EXCEPT !.cache=FALSE, !.native=0, !.charged=0,
                   !.validated=FALSE]
Next == \/ \E c \in Clients: ColdOpen(c) \/ AdmitValidation(c)
          \/ ValidationChunk(c) \/ PublishValidated(c) \/ AbandonValidation(c)
          \/ WarmPin(c) \/ BeginRead(c) \/ EndRead(c) \/ DropPin(c)
          \/ ReturnMapped(c) \/ Stop(c)
        \/ \E c,d \in Clients: ClonePin(c,d)
        \/ Evict
Spec == Init /\ [][Next]_vars

TypeOK == /\ s.native \in 0..1 /\ s.charged \in 0..1
 /\ s.cache \in BOOLEAN /\ s.admitted \in BOOLEAN /\ s.validated \in BOOLEAN
 /\ s.pins \subseteq Clients /\ s.reads \subseteq Clients
 /\ s.verified \subseteq Clients /\ s.returned \subseteq Clients
 /\ s.stopped \subseteq Clients /\ s.owner \in Clients \cup {0}
 /\ s.chunks \in 0..2
DescriptorCovered == s.native <= s.charged
LeaseCovered == (s.pins # {} \/ s.reads # {} \/ s.owner # 0)
                => s.native=1 /\ s.charged=1
CachedValidated == s.cache => s.validated
PayloadValidated == s.pins \cup s.reads \cup s.returned \subseteq s.verified
NoPinAfterReturn == s.returned \cap (s.pins \cup s.reads) = {}
NativeHasOwner == (s.native=1) <=>
                   (s.cache \/ s.pins # {} \/ s.reads # {} \/ s.owner # 0)
=============================================================================
