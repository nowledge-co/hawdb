---------------- MODULE HawDBCapturedDirectoryRelabel ----------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS SetupRows, Platforms, FinishCases, NativeReplies, ObserverReplies,
          RaceChoices, Mutation
VARIABLES x, history
vars == <<x, history>>

Parents == {"P", "Q"}
Names == {"src", "dst", "away"}
Entries == {"None", "Original", "Foreign", "Decoy", "OccupiedDir", "OccupiedFile", "OccupiedLink", "Link"}
Owners == {"parent", "child", "anchor"}
AllRows == {"valid", "label_redirect", "foreign_source", "source_link",
            "occupied_dir", "occupied_file", "alias", "fd_refusal"}
AllFinish == {"Stable", "FreshChanged", "StickyRestored", "FDOnce", "SyncOnce"}
AllMutations == {"none", "wrong_parent_fd", "omit_child_identity",
 "follow_source_link", "replace_target", "omit_alias_precheck",
 "late_attempt_marker", "late_native_completion", "reset_uncertain",
 "replay_consumed", "recorder_path_reopen", "early_durability",
 "weak_native_owners", "clear_binding_fault"}

EmptyParent == [n \in Names |-> "None"]
InitialNS(row, targetKind) ==
 [p \in Parents |->
   IF p = "Q" THEN [EmptyParent EXCEPT !["src"] = "Decoy"]
   ELSE [n \in Names |->
     CASE n = "src" -> (CASE row = "foreign_source" -> "Foreign"
                            [] row = "source_link" -> "Link"
                            [] OTHER -> "Original")
       [] n = "away" -> IF row \in {"foreign_source", "source_link"}
                         THEN "Original" ELSE "None"
       [] OTHER -> (CASE row = "occupied_dir" -> "OccupiedDir"
                        [] row = "occupied_file" -> targetKind
                        [] OTHER -> "None")]]

(* Alias lookup is equivalence of spellings of ONE native entry; the map has
   only its src edge. It does not create directory hard links. *)
Lookup(ns, p, n, alias) ==
 IF alias /\ p = "P" /\ n = "dst" THEN ns[p]["src"] ELSE ns[p][n]
Binding(ns) == ns["P"]["src"] = "None" /\ ns["P"]["dst"] = "Original"
ResolvedSource == IF x.ns["P"]["src"] = "Link"
                  THEN x.ns["P"]["away"] ELSE x.ns["P"]["src"]
SourceAccepted ==
 IF Mutation = "follow_source_link" THEN ResolvedSource = "Original"
 ELSE IF Mutation = "omit_child_identity"
      THEN x.ns["P"]["src"] \in {"Original", "Foreign"}
      ELSE x.ns["P"]["src"] = "Original"
TargetAccepted ==
 Lookup(x.ns, "P", "dst", x.alias) = "None"
 \/ (Mutation = "omit_alias_precheck" /\ x.alias)

(* Events are observations of actual producer calls/results and actual namespace
   snapshots. Mutations change producer choices, never these oracles. *)
Event(kind) == [kind |-> kind, parent |-> "P", sourceEntry |-> "None",
 effectChild |-> "None", targetBefore |-> "None", effect |-> FALSE,
 replySuccess |-> FALSE, attempted |-> x.attempted, known |-> x.known,
 owners |-> x.owners, helperFD |-> x.helperFD, binding |-> Binding(x.ns), sync |-> FALSE,
 ack |-> FALSE, result |-> "none", recordChild |-> "None"]
Emit(e) == history' = Append(history, e)

Init ==
 \E row \in SetupRows, platform \in Platforms, finishCase \in FinishCases,
    nativeReply \in NativeReplies, observerReply \in ObserverReplies,
    race \in RaceChoices, targetKind \in {"OccupiedFile", "OccupiedLink"} :
 x = [row |-> row, platform |-> platform, finishCase |-> finishCase,
      nativeReply |-> nativeReply, observerReply |-> observerReply, race |-> race,
      ns |-> InitialNS(row, targetKind), recorded |-> InitialNS(row, targetKind),
      alias |-> row = "alias", labelParent |-> IF row = "label_redirect" THEN "Q" ELSE "P",
      parentCapture |-> "P", childCapture |-> "Original",
      owners |-> Owners, phase |-> "Fresh", attempted |-> FALSE, known |-> FALSE,
      nativeSuccess |-> FALSE, nativeCount |-> 0, replayCount |-> 0,
      sticky |-> FALSE, finishCount |-> 0, helperFD |-> 0,
      changed |-> FALSE, restored |-> FALSE, durable |-> FALSE]
 /\ history = <<>>

(* Outer Arc releases do not close originals retained by the candidate. The
   mutant drops its weak/scalar custody instead. Caller namespace exclusion is
   a real environmental precondition through Preflight/Kernel/Reply, not a
   capability minted by any Boolean or this model. *)
Prepare ==
 /\ x.phase = "Fresh"
 /\ x' = [x EXCEPT !.phase = "Preflight",
          !.owners = IF Mutation = "weak_native_owners" THEN {} ELSE @]
 /\ UNCHANGED history

PreflightRefusal ==
 /\ x.phase = "Preflight"
 /\ (x.row = "fd_refusal" \/ ~SourceAccepted \/ ~TargetAccepted)
 /\ x' = [x EXCEPT !.phase = "Done"]
 /\ Emit([Event("Refusal") EXCEPT !.result = "pre_effect_refusal"])

PreflightAdmit ==
 /\ x.phase = "Preflight" /\ x.row # "fd_refusal"
 /\ SourceAccepted /\ TargetAccepted
 /\ x' = [x EXCEPT !.phase = "Kernel", !.helperFD = 1,
         !.attempted = Mutation # "late_attempt_marker",
         !.ns = IF x.race /\ ~x.alias
                 THEN [@ EXCEPT !["P"]["dst"] = "OccupiedDir"] ELSE @]
 /\ UNCHANGED history

Kernel ==
 /\ x.phase = "Kernel"
 /\ LET p == IF Mutation = "wrong_parent_fd" THEN x.labelParent ELSE "P"
        sourceEntry == x.ns[p]["src"]
        sourceSlot == IF x.platform = "Windows" /\ Mutation = "follow_source_link"
                         /\ sourceEntry = "Link" THEN "away" ELSE "src"
        child == x.ns[p][sourceSlot]
        target == Lookup(x.ns, p, "dst", x.alias)
        occupied == target # "None"
        aliasNoOp == x.alias /\ p = "P" /\ Mutation = "omit_alias_precheck"
        available == x.owners = Owners /\ x.helperFD = 1
        succeeds == available /\ x.nativeReply # "NoEffectError"
                    /\ (~occupied \/ Mutation = "replace_target" \/ aliasNoOp)
        effect == succeeds /\ ~aliasNoOp
        actualNS == IF effect THEN [x.ns EXCEPT ![p][sourceSlot] = "None",
                                                       ![p]["dst"] = child] ELSE x.ns
     IN /\ x' = [x EXCEPT !.phase = "Reply", !.ns = actualNS,
              !.nativeCount = @ + 1,
              !.nativeSuccess = succeeds /\ x.nativeReply = "MovedSuccess"]
        /\ Emit([Event("Kernel") EXCEPT !.parent = p,
            !.sourceEntry = sourceEntry, !.effectChild = child,
            !.targetBefore = target, !.effect = effect,
            !.replySuccess = succeeds /\ x.nativeReply = "MovedSuccess"])

Reply ==
 /\ x.phase = "Reply"
 /\ x' = [x EXCEPT !.phase = IF x.nativeSuccess THEN "Record" ELSE "Replay",
       !.known = x.nativeSuccess /\ Mutation # "late_native_completion",
       !.attempted = IF ~x.nativeSuccess /\ Mutation = "reset_uncertain"
                     THEN FALSE ELSE TRUE]
 /\ Emit([Event("Reply") EXCEPT !.replySuccess = x.nativeSuccess])

(* Atomic recorder mutation addresses captured parent + expected original child.
   Namespace image commits only on a native successful reply. Its independent
   image is not the native namespace and does not stand in for actual disk. *)
Record ==
 /\ x.phase = "Record"
 /\ LET p == IF Mutation = "recorder_path_reopen" THEN x.labelParent ELSE "P"
        child == x.recorded[p]["src"]
        accepted == child = "Original" /\ x.recorded[p]["dst"] = "None"
     IN /\ x' = [x EXCEPT !.phase = "Observe",
             !.recorded = IF accepted
               THEN [@ EXCEPT ![p]["src"] = "None", ![p]["dst"] = child] ELSE @]
        /\ Emit([Event("Record") EXCEPT !.parent = p,
             !.recordChild = child, !.result = IF accepted THEN "commit" ELSE "refuse"])

Observe ==
 /\ x.phase = "Observe"
 /\ x' = [x EXCEPT !.phase = IF x.observerReply = "ok" THEN "Verify" ELSE "Done",
         !.known = IF x.observerReply = "ok" THEN TRUE ELSE @,
         !.helperFD = 0]
 /\ Emit([Event("Observe") EXCEPT !.replySuccess = x.nativeSuccess,
                !.result = x.observerReply])

Verify ==
 /\ x.phase = "Verify"
 /\ x' = [x EXCEPT !.phase = IF Binding(x.ns) THEN "BeforeFinish" ELSE "Done",
                !.sticky = ~Binding(x.ns)]
 /\ Emit([Event("Verify") EXCEPT !.result = IF Binding(x.ns) THEN "ok" ELSE "binding_fault"])

(* Only inter-call changes by the SAME caller occur here. No arbitrary external
   source swap is allowed between preflight and the native operation. *)
BeforeFinish ==
 /\ x.phase = "BeforeFinish"
 /\ x' = [x EXCEPT !.phase = "Finish",
       !.changed = x.finishCase \in {"FreshChanged", "StickyRestored"},
       !.ns = IF x.finishCase \in {"FreshChanged", "StickyRestored"}
              THEN [@ EXCEPT !["P"]["dst"] = "None", !["P"]["away"] = "Original"] ELSE @]
 /\ UNCHANGED history

Finish ==
 /\ x.phase = "Finish" /\ x.finishCount < 2
 /\ LET fdRefusal == x.finishCase = "FDOnce" /\ x.finishCount = 0
        syncRefusal == x.finishCase = "SyncOnce" /\ x.finishCount = 0
        bindingFault == ~Binding(x.ns)
        rejectedBinding == Mutation # "early_durability" /\ (bindingFault \/ x.sticky)
        attemptedSync == ~fdRefusal /\ ~rejectedBinding
        actualSync == attemptedSync /\ ~syncRefusal
        ack == (Mutation = "early_durability")
               \/ (~fdRefusal /\ ~rejectedBinding /\ actualSync)
        result == IF ack THEN "ok" ELSE IF fdRefusal THEN "fd_refusal"
                  ELSE IF rejectedBinding THEN "binding_fault" ELSE "sync_refusal"
     IN /\ x' = [x EXCEPT !.phase = "AfterFinish", !.finishCount = @ + 1,
             !.sticky = x.sticky \/ (bindingFault /\ ~fdRefusal),
             !.durable = @ \/ ack, !.helperFD = 0]
        /\ Emit([Event("Finish") EXCEPT !.binding = Binding(x.ns),
                !.sync = actualSync, !.helperFD = IF attemptedSync THEN 1 ELSE 0,
                !.ack = ack, !.result = result])

AfterFinish ==
 /\ x.phase = "AfterFinish"
 /\ LET restore == x.finishCase = "StickyRestored" /\ x.finishCount = 1
        retry == x.finishCount = 1 /\ x.finishCase \in {"StickyRestored", "FDOnce", "SyncOnce"}
     IN /\ x' = [x EXCEPT !.phase = IF retry THEN "Finish" ELSE "Done",
            !.ns = IF restore THEN [@ EXCEPT !["P"]["away"] = "None",
                                                    !["P"]["dst"] = "Original"] ELSE @,
            !.restored = restore \/ @,
            !.sticky = IF restore /\ Mutation = "clear_binding_fault" THEN FALSE ELSE @]
        /\ UNCHANGED history

(* A bounded second public invocation is modeled even after an uncertain reply.
   Correct code refuses it; the mutant actually reenters the kernel. *)
Replay ==
 /\ x.phase = "Replay" /\ x.replayCount = 0
 /\ x' = [x EXCEPT !.replayCount = 1,
        !.phase = IF Mutation = "replay_consumed" THEN "Kernel" ELSE "Done",
        !.helperFD = IF Mutation = "replay_consumed" THEN 1 ELSE 0]
 /\ UNCHANGED history
ReplayEnd ==
 /\ x.phase = "Replay" /\ x.replayCount = 1
 /\ x' = [x EXCEPT !.phase = "Done", !.helperFD = 0]
 /\ UNCHANGED history

Next == Prepare \/ PreflightRefusal \/ PreflightAdmit \/ Kernel \/ Reply
        \/ Record \/ Observe \/ Verify \/ BeforeFinish \/ Finish \/ AfterFinish
        \/ Replay \/ ReplayEnd
Spec == Init /\ [][Next]_vars

TypeOK == /\ x.row \in AllRows /\ x.platform \in {"Unix", "Windows"}
 /\ x.finishCase \in AllFinish /\ Mutation \in AllMutations
 /\ x.ns \in [Parents -> [Names -> Entries]]
 /\ x.recorded \in [Parents -> [Names -> Entries]]
 /\ x.owners \subseteq Owners /\ x.helperFD \in 0..1
 /\ x.nativeCount \in 0..2 /\ x.replayCount \in 0..1 /\ x.finishCount \in 0..2
 /\ Len(history) <= 12
OriginalParentAndChild ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Kernel" /\ e.effect) =>
   (e.parent = x.parentCapture /\ e.sourceEntry = x.childCapture
    /\ e.effectChild = x.childCapture)
ExclusiveTarget ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Kernel" /\ (e.effect \/ e.replySuccess)) => e.targetBefore = "None"
AttemptBeforeEffect ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Kernel" /\ (e.effect \/ e.replySuccess)) => e.attempted
MonotoneEvidence ==
 /\ ((\E i \in 1..Len(history): history[i].kind = "Kernel") => x.attempted)
 /\ \A i \in 1..Len(history): LET e == history[i] IN
      (e.kind = "Observe" /\ e.replySuccess) => e.known
OneNativeAttempt == x.nativeCount <= 1
LiveOriginalOwners ==
 \A i \in 1..Len(history): history[i].kind = "Kernel" => history[i].owners = Owners
RecordedOriginalInode ==
 \A i \in 1..Len(history): LET e == history[i] IN
  e.kind = "Record" => (e.parent = x.parentCapture /\ e.recordChild = x.childCapture)
DurableOnlyAfterBarrier ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Finish" /\ e.ack) => e.sync
FreshFinishBinding ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Finish" /\ e.ack) => e.binding
StickyBindingFault ==
 \A i \in 1..Len(history), j \in 1..Len(history):
  (i < j /\ history[i].kind \in {"Verify", "Finish"}
   /\ history[i].result = "binding_fault" /\ history[j].kind = "Finish")
   => ~history[j].ack
ResourceRetryRetainsOwners ==
 \A i \in 1..Len(history): LET e == history[i] IN
  (e.kind = "Finish" /\ e.result \in {"fd_refusal", "sync_refusal"})
   => (e.owners = Owners /\ e.known)

(* Non-vacuity predicates are checked as expected-failing diagnostic invariants
   in separate witness configurations, never mixed into baseline correctness. *)
NoSuccessfulStable == ~ (x.phase = "Done" /\ x.finishCase = "Stable" /\ x.durable)
NoSuccessfulFDRecover == ~ (x.finishCase = "FDOnce" /\ x.finishCount = 2 /\ x.durable)
NoSuccessfulSyncRecover == ~ (x.finishCase = "SyncOnce" /\ x.finishCount = 2 /\ x.durable)
NoStickyRestoredRefusal == ~ (x.phase = "Done" /\ x.finishCase = "StickyRestored"
 /\ x.restored /\ x.known /\ ~x.durable /\ x.finishCount = 2)
NoLostReplyRetained == ~ (x.phase = "Done" /\ x.nativeReply = "MovedError"
 /\ x.attempted /\ ~x.known /\ x.owners = Owners
 /\ x.ns["P"]["dst"] = "Original")
=============================================================================
