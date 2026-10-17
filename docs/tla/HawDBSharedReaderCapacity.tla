---------------- MODULE HawDBSharedReaderCapacity ----------------
EXTENDS Naturals, FiniteSets

CONSTANTS Clients, RefundBeforeBlock, DetachClone, KeepExecutionPermit, RefundBeforeLedger
ASSUME /\ IsFiniteSet(Clients) /\ Clients # {}
       /\ RefundBeforeBlock \in BOOLEAN /\ DetachClone \in BOOLEAN
       /\ KeepExecutionPermit \in BOOLEAN
       /\ RefundBeforeLedger \in BOOLEAN

(* One projection, one fixed-size account ledger, finite single-use clone slots.
   Rust Arc::into_inner is abstracted at its atomic strong decrement: exactly
   the last decrement extracts T and frees the block, with no weak/raw escape.
   This is a resource protocol model, not a proof of Arc, allocator byte bounds,
   immutable source custody, complete reader admission or MVCC activation. *)
VARIABLE s
vars == <<s>>
First == CHOOSE c \in Clients: TRUE
LiveBytes == (IF s.block THEN 1 ELSE 0)
           + (IF s.payload THEN 2 ELSE 0)
           + (IF s.ledger THEN 1 ELSE 0) + 2 * Cardinality(s.detached)

Init == s = [phase |-> "idle", query |-> FALSE, host |-> FALSE,
 ledger |-> TRUE, accountRef |-> FALSE, block |-> FALSE, payload |-> FALSE,
 live |-> {}, dropping |-> {}, used |-> {}, extracted |-> 0,
 producerClosed |-> FALSE, executionHeld |-> TRUE, detached |-> {}]

QueryReserve == /\ s.phase="idle"
 /\ s'=[s EXCEPT !.phase="query", !.query=TRUE, !.accountRef=TRUE]
HostReserve == /\ s.phase="query"
 /\ s'=[s EXCEPT !.phase="admitted", !.host=TRUE]
HostRefuse == /\ s.phase="query"
 /\ s'=[s EXCEPT !.phase="rollback"]
Rollback == /\ s.phase="rollback"
 /\ s'=[s EXCEPT !.phase="done", !.query=FALSE, !.accountRef=FALSE]
Allocate == /\ s.phase="admitted"
 /\ s'=[s EXCEPT !.phase="shared", !.block=TRUE, !.payload=TRUE,
         !.live={First}, !.used={First}]
Abort == /\ s.phase="admitted"
 /\ s'=[s EXCEPT !.phase="payloadFreed"]
Clone(c) == /\ s.phase="shared" /\ s.live # {} /\ c \notin s.used
 /\ s'=[s EXCEPT !.live=@ \cup {c}, !.used=@ \cup {c},
         !.detached=IF DetachClone THEN @ \cup {c} ELSE @]
BeginDrop(c) == /\ c \in s.live
 /\ s'=[s EXCEPT !.live=@ \ {c}, !.dropping=@ \cup {c}]
Decrement(c) == /\ c \in s.dropping
 /\ LET last == s.live={} /\ s.dropping={c}
    IN s'=[s EXCEPT !.dropping=@ \ {c},
         !.detached=@ \ {c},
         !.phase=IF last THEN "extracted" ELSE @,
         !.extracted=@+IF last THEN 1 ELSE 0,
         !.block=IF last /\ ~RefundBeforeBlock THEN FALSE ELSE @,
         !.host=IF last /\ RefundBeforeBlock THEN FALSE ELSE @]
FreePayload == /\ s.phase="extracted"
 /\ s'=[s EXCEPT !.phase="payloadFreed", !.payload=FALSE]
ReleaseQuery == /\ s.phase="payloadFreed"
 /\ s'=[s EXCEPT !.phase="queryReleased", !.query=FALSE,
         !.host=IF RefundBeforeLedger THEN FALSE ELSE @]
DropAccount == /\ s.phase="queryReleased"
 /\ s'=[s EXCEPT !.phase="accountFreed", !.accountRef=FALSE,
         !.ledger=~s.producerClosed]
ReleaseHost == /\ s.phase="accountFreed"
 /\ s'=[s EXCEPT !.phase="done", !.host=FALSE]
CloseProducer == /\ ~s.producerClosed
 /\ s.phase \in {"shared","extracted","payloadFreed","queryReleased","accountFreed","done"}
 /\ s'=[s EXCEPT !.producerClosed=TRUE,
         !.ledger=s.accountRef,
         !.executionHeld=KeepExecutionPermit /\ s.host]

Next == QueryReserve \/ HostReserve \/ HostRefuse \/ Rollback \/ Allocate
 \/ Abort \/ (\E c \in Clients: Clone(c) \/ BeginDrop(c) \/ Decrement(c))
 \/ FreePayload \/ ReleaseQuery \/ DropAccount \/ ReleaseHost \/ CloseProducer

TypeOK == /\ s.phase \in {"idle","query","admitted","rollback","shared",
                          "extracted","payloadFreed","queryReleased","accountFreed","done"}
 /\ s.live \subseteq Clients /\ s.dropping \subseteq Clients
 /\ s.used \subseteq Clients /\ s.live \cap s.dropping={}
 /\ (s.live \cup s.dropping) \subseteq s.used
 /\ s.extracted \in 0..1 /\ s.detached \subseteq (s.live \cup s.dropping)
 /\ \A k \in {"query","host","ledger","accountRef","block","payload","producerClosed",
               "executionHeld"}: s[k] \in BOOLEAN
LedgerLifetime == s.ledger=(~s.producerClosed \/ s.accountRef)
 /\ (s.query => s.accountRef)
BackingCovered == s.block \/ s.payload => s.query /\ s.host
LedgerCovered == s.ledger => s.host \/ ~s.producerClosed
CapacityCovered == s.host => LiveBytes <= 4
ReferencesCovered == s.live \cup s.dropping # {} => s.block /\ s.payload
NoExecutionAfterClose == s.producerClosed => ~s.executionHeld
NoTerminalLeak == s.phase="done" =>
 ~s.query /\ ~s.host /\ ~s.block /\ ~s.payload /\ ~s.accountRef
 /\ (s.producerClosed => ~s.ledger) /\ s.detached={}
SingleExtraction == s.phase \in {"extracted","payloadFreed","queryReleased","accountFreed","done"}
 /\ s.used # {} => s.extracted=1

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)
EventuallyDisposed == <>(s.phase="done" /\ s.producerClosed)
====================================================================
