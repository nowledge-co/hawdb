---------------- MODULE HawDBBorrowedQueryCursor ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Queries, ReuseTask, DropBorrow, SkipCheckpoint
ASSUME /\ IsFiniteSet(Queries) /\ Queries # {}
       /\ ReuseTask \in BOOLEAN /\ DropBorrow \in BOOLEAN
       /\ SkipCheckpoint \in BOOLEAN

(* One immutable view survives its producer. Tasks may outlive permits; a
   retained task alone is not execution authority. A cursor borrows both its
   view and serving permit, and I/O is scoped within that cursor. Single-use
   query IDs bound this safety model; no fairness or memory-byte proof. *)
VARIABLE s
vars == <<s>>
Init == s = [phase |-> [q \in Queries |-> "new"], cursor |-> {}, io |-> {},
             cancelled |-> {}, deliveredCancelled |-> FALSE, producer |-> TRUE]
Active == {q \in Queries : s.phase[q]="active"}
Tasks == {q \in Queries : s.phase[q] \in {"active","taskOnly"}}
Admit(q) == /\ s.phase[q]="new"
 /\ s'=[s EXCEPT !.phase[q]="active"]
CloseProducer == /\ s.producer /\ s'=[s EXCEPT !.producer=FALSE]
OpenCursor(q) == /\ q \notin s.cursor
 /\ (q \in Active \/ (ReuseTask /\ q \in Tasks))
 /\ s'=[s EXCEPT !.cursor=@ \cup {q}]
BeginIo(q) == /\ q \in s.cursor /\ q \notin s.cancelled /\ s.io={}
 /\ s'=[s EXCEPT !.io={q}]
Cancel(q) == /\ q \in Tasks /\ q \notin s.cancelled
 /\ s'=[s EXCEPT !.cancelled=@ \cup {q}]
CompleteIo(q) == /\ q \in s.io
 /\ s'=[s EXCEPT !.io=@ \ {q},
      !.deliveredCancelled=@ \/ (SkipCheckpoint /\ q \in s.cancelled)]
CloseCursor(q) == /\ q \in s.cursor /\ q \notin s.io
 /\ s'=[s EXCEPT !.cursor=@ \ {q}]
ClosePermit(q) == /\ s.phase[q]="active"
 /\ (q \notin s.cursor \/ DropBorrow)
 /\ s'=[s EXCEPT !.phase[q]="taskOnly"]
CloseTask(q) == /\ s.phase[q]="taskOnly" /\ q \notin s.cursor
 /\ s'=[s EXCEPT !.phase[q]="closed"]
Next == CloseProducer \/ (\E q \in Queries:
 Admit(q) \/ OpenCursor(q) \/ BeginIo(q) \/ Cancel(q) \/ CompleteIo(q)
 \/ CloseCursor(q) \/ ClosePermit(q) \/ CloseTask(q))
TypeOK == /\ s.phase \in [Queries -> {"new","active","taskOnly","closed"}]
 /\ s.cursor \subseteq Queries /\ s.io \subseteq Queries
 /\ s.cancelled \subseteq Queries /\ Cardinality(s.io)<=1
 /\ s.producer \in BOOLEAN /\ s.deliveredCancelled \in BOOLEAN
CursorBorrowed == s.cursor \subseteq Active
IoBorrowed == s.io \subseteq (s.cursor \cap Active)
NoCancelledDelivery == ~s.deliveredCancelled
Spec == Init /\ [][Next]_vars
====================================================================
