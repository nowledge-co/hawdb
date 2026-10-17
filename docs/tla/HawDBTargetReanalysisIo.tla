---------------- MODULE HawDBTargetReanalysisIo ----------------
EXTENDS Naturals
CONSTANTS Chunks, KeepCaptureIo, KeepBodyIo
ASSUME /\ Chunks \in Nat \ {0}
       /\ KeepCaptureIo \in BOOLEAN /\ KeepBodyIo \in BOOLEAN

(* One query, one I/O slot, a captured source and a term cursor borrowing its
   execution. Each body read returns its slot before a term callback can read.
   Two symbolic chunks bound the finite check; no cache/lock fairness model. *)
VARIABLE s
vars == <<s>>
Terminal == {"done", "aborted"}
Init == s=[phase |-> "idle", io |-> "none", remaining |-> Chunks,
 body |-> FALSE, terms |-> FALSE, execution |-> TRUE,
 cancelled |-> FALSE, waiting |-> FALSE]
Start == /\ s.phase="idle" /\ ~s.cancelled
 /\ s'=[s EXCEPT !.phase="capture", !.io="capture"]
CaptureEnd == /\ s.phase="capture" /\ ~s.cancelled
 /\ s'=[s EXCEPT !.phase="body", !.body=TRUE, !.terms=TRUE,
                  !.io=IF KeepCaptureIo THEN "capture" ELSE "none"]
BeginBody == /\ s.phase="body" /\ s.remaining>0 /\ ~s.cancelled
 /\ s'=IF s.io="none" THEN [s EXCEPT !.phase="bodyRead", !.io="body"]
        ELSE [s EXCEPT !.waiting=TRUE]
BodyEnd == /\ s.phase="bodyRead" /\ ~s.cancelled
 /\ s'=[s EXCEPT !.phase="emit", !.io=IF KeepBodyIo THEN "body" ELSE "none"]
BeginTerm == /\ s.phase="emit" /\ ~s.cancelled
 /\ s'=IF s.io="none" THEN [s EXCEPT !.phase="termRead", !.io="term"]
        ELSE [s EXCEPT !.waiting=TRUE]
TermEnd == /\ s.phase="termRead" /\ ~s.cancelled
 /\ s'=[s EXCEPT !.phase="body", !.io="none", !.remaining=@-1]
BeginDigest == /\ s.phase="body" /\ s.remaining=0 /\ ~s.cancelled
 /\ s'=IF s.io="none" THEN [s EXCEPT !.phase="digest", !.io="digest"]
        ELSE [s EXCEPT !.waiting=TRUE]
DigestEnd == /\ s.phase="digest" /\ ~s.cancelled
 /\ s'=[s EXCEPT !.phase="done", !.io="none", !.body=FALSE, !.terms=FALSE]
Cancel == /\ s.phase \notin Terminal /\ ~s.cancelled
 /\ s'=[s EXCEPT !.cancelled=TRUE]
Abort == /\ s.phase \notin Terminal /\ s.cancelled
 /\ s'=[s EXCEPT !.phase="aborted", !.io="none", !.body=FALSE, !.terms=FALSE]
CloseExecution == /\ s.phase \in Terminal /\ s.execution
 /\ s'=[s EXCEPT !.execution=FALSE]
Next == Start \/ CaptureEnd \/ BeginBody \/ BodyEnd \/ BeginTerm \/ TermEnd
 \/ BeginDigest \/ DigestEnd \/ Cancel \/ Abort \/ CloseExecution
TypeOK == /\ s.phase \in {"idle","capture","body","bodyRead","emit","termRead","digest","done","aborted"}
 /\ s.io \in {"none","capture","body","term","digest"}
 /\ s.remaining \in 0..Chunks
 /\ \A k \in {"body","terms","execution","cancelled","waiting"}: s[k] \in BOOLEAN
NoSelfWait == ~s.waiting
ExecutionCovered == (s.body \/ s.terms \/ s.io#"none") => s.execution
TerminalReleased == s.phase \in Terminal => ~s.body /\ ~s.terms /\ s.io="none"
Spec == Init /\ [][Next]_vars
====================================================================
