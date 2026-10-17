---------------- MODULE HawDBSearchBatchCoverage ----------------
EXTENDS Integers, FiniteSets, Sequences, TLC
CONSTANTS MaxEpoch, PublishPrefix, SkipItem, IgnoreMapperError, DropDeletes,
          WrongSnapshot, SubstituteStage, IgnoreDependency, IgnoreBinding,
          IgnoreInstance, IgnoreSchema, IgnoreBase, ReleaseFence
VARIABLES history, log, phase, sourceEpoch, cursor, sourceWorld, authority,
          capturedAuthority, rows, deletes, covered, visible, watermark,
          bad, completeProgress, disjointProgress, postDisjoint
vars == <<history, log, phase, sourceEpoch, cursor, sourceWorld, authority,
          capturedAuthority, rows, deletes, covered, visible, watermark,
          bad, completeProgress, disjointProgress, postDisjoint>>
Roots == {1,2}
InitialWorld == [n \in Roots |-> [alive |-> TRUE, id |-> n*2, payload |-> 0]]
InitialAuthority == [instance |-> 0, schema |-> 0, binding |-> 0, base |-> 0]
Tip == Len(history)-1
Current == history[Len(history)]
NodeRows(world,n) == IF world[n].alive THEN {<<world[n].id,world[n].payload>>} ELSE {}
Projection(world) == UNION {NodeRows(world,n): n \in Roots}
BaseIndex == Projection(InitialWorld)
After(world,n,kind) == CASE
    kind = "payload" -> [world EXCEPT ![n].payload = 1-@]
 [] kind = "rename" -> [world EXCEPT ![n].id = IF @ = n*2 THEN n*2-1 ELSE n*2]
 [] OTHER -> [world EXCEPT ![n].alive = ~@]
Affected == {log[i].node: i \in 1..sourceEpoch}
DependenciesValid == \A i \in (sourceEpoch+1)..Tip: log[i].node \notin Affected
AuthorityValid == authority = capturedAuthority
FenceAllowsMutation == phase # "validated" \/ ReleaseFence
CanWrite == FenceAllowsMutation /\ phase \notin {"published","failed"}
Init == /\ history = <<InitialWorld>> /\ log = <<>> /\ phase = "idle"
 /\ sourceEpoch = 0 /\ cursor = 0 /\ sourceWorld = InitialWorld
 /\ authority = InitialAuthority /\ capturedAuthority = InitialAuthority
 /\ rows = {} /\ deletes = {} /\ covered = {}
 /\ visible = BaseIndex /\ watermark = 0 /\ bad = FALSE
 /\ completeProgress = FALSE /\ disjointProgress = FALSE /\ postDisjoint = FALSE
Write(n,kind) ==
 /\ CanWrite /\ Tip < MaxEpoch /\ n \in Roots
 /\ kind \in {"payload","rename","presence"}
 /\ Current[n].alive \/ kind = "presence"
 /\ history' = Append(history,After(Current,n,kind))
 /\ log' = Append(log,[node |-> n,
       old |-> IF kind \in {"rename","presence"} /\ Current[n].alive
               THEN {Current[n].id} ELSE {}])
 /\ postDisjoint' = (postDisjoint \/ (phase # "idle" /\ n \notin Affected))
 /\ UNCHANGED <<phase,sourceEpoch,cursor,sourceWorld,authority,capturedAuthority,
                 rows,deletes,covered,visible,watermark,bad,completeProgress,disjointProgress>>
ChangeAuthority(field) ==
 /\ CanWrite /\ field \in {"instance","schema","binding","base"}
 /\ authority[field] = 0
 /\ authority' = [authority EXCEPT ![field] = 1]
 /\ UNCHANGED <<history,log,phase,sourceEpoch,cursor,sourceWorld,capturedAuthority,
                 rows,deletes,covered,visible,watermark,bad,completeProgress,disjointProgress,postDisjoint>>
Capture ==
 /\ phase = "idle"
 /\ sourceEpoch' = Tip /\ sourceWorld' = Current
 /\ capturedAuthority' = authority /\ phase' = "building"
 /\ UNCHANGED <<history,log,cursor,authority,rows,deletes,covered,visible,watermark,
                 bad,completeProgress,disjointProgress,postDisjoint>>
Visit(failure) ==
 /\ phase = "building" /\ cursor < sourceEpoch /\ failure \in BOOLEAN
 /\ LET refused == failure /\ ~IgnoreMapperError
        record == log[cursor+1]
        world == IF WrongSnapshot THEN InitialWorld ELSE sourceWorld
        omitted == SkipItem \/ failure
    IN /\ phase' = (IF refused THEN "failed" ELSE phase)
       /\ cursor' = (IF refused THEN cursor ELSE cursor+1)
       /\ covered' = (IF refused THEN covered ELSE covered \cup {cursor+1})
       /\ rows' = (IF refused \/ omitted THEN rows ELSE rows \cup NodeRows(world,record.node))
       /\ deletes' = (IF refused \/ omitted \/ DropDeletes THEN deletes ELSE deletes \cup record.old)
 /\ UNCHANGED <<history,log,sourceEpoch,sourceWorld,authority,capturedAuthority,
                 visible,watermark,bad,completeProgress,disjointProgress,postDisjoint>>
Seal ==
 /\ phase = "building" /\ (cursor = sourceEpoch \/ PublishPrefix)
 /\ phase' = "sealed"
 /\ UNCHANGED <<history,log,sourceEpoch,cursor,sourceWorld,authority,capturedAuthority,
                 rows,deletes,covered,visible,watermark,bad,completeProgress,disjointProgress,postDisjoint>>
ChecksPass ==
 /\ (IgnoreInstance \/ authority.instance = capturedAuthority.instance)
 /\ (IgnoreSchema \/ authority.schema = capturedAuthority.schema)
 /\ (IgnoreBinding \/ authority.binding = capturedAuthority.binding)
 /\ (IgnoreBase \/ authority.base = capturedAuthority.base)
 /\ (IgnoreDependency \/ DependenciesValid)
Validate ==
 /\ phase = "sealed" /\ ChecksPass /\ phase' = "validated"
 /\ UNCHANGED <<history,log,sourceEpoch,cursor,sourceWorld,authority,capturedAuthority,
                 rows,deletes,covered,visible,watermark,bad,completeProgress,disjointProgress,postDisjoint>>
Reduced == {row \in BaseIndex: row[1] \notin deletes \cup {r[1]: r \in rows}} \cup rows
Publish ==
 /\ phase = "validated"
 /\ LET permitted == IF ReleaseFence THEN TRUE ELSE ChecksPass
        stage == IF SubstituteStage THEN [session |-> 2, output |-> BaseIndex]
                 ELSE [session |-> 1, output |-> Reduced]
        output == stage.output
        safe == stage.session = 1 /\ AuthorityValid /\ DependenciesValid /\ cursor = sourceEpoch
                /\ covered = 1..sourceEpoch /\ output = Projection(sourceWorld)
    IN /\ phase' = (IF permitted THEN "published" ELSE "failed")
       /\ visible' = (IF permitted THEN output ELSE visible)
       /\ watermark' = (IF permitted THEN sourceEpoch ELSE watermark)
       /\ bad' = (bad \/ (permitted /\ ~safe))
       /\ completeProgress' = (completeProgress \/ (permitted /\ safe /\ sourceEpoch > 0))
       /\ disjointProgress' = (disjointProgress \/ (permitted /\ safe /\ postDisjoint /\ sourceEpoch > 0))
 /\ UNCHANGED <<history,log,sourceEpoch,cursor,sourceWorld,authority,capturedAuthority,
                 rows,deletes,covered,postDisjoint>>
Cancel ==
 /\ phase \in {"building","sealed"} /\ phase' = "failed"
 /\ rows' = {} /\ deletes' = {}
 /\ UNCHANGED <<history,log,sourceEpoch,cursor,sourceWorld,authority,capturedAuthority,
                 covered,visible,watermark,bad,completeProgress,disjointProgress,postDisjoint>>
Next == (\E n \in Roots,k \in {"payload","rename","presence"}: Write(n,k))
 \/ (\E field \in {"instance","schema","binding","base"}: ChangeAuthority(field))
 \/ Capture \/ (\E failure \in BOOLEAN: Visit(failure)) \/ Seal \/ Validate \/ Publish \/ Cancel
TypeOK == /\ Tip \in 0..MaxEpoch /\ Len(log) = Tip
 /\ sourceEpoch \in 0..Tip /\ cursor \in 0..sourceEpoch
 /\ phase \in {"idle","building","sealed","validated","published","failed"}
 /\ covered \subseteq 1..sourceEpoch /\ bad \in BOOLEAN
 /\ completeProgress \in BOOLEAN /\ disjointProgress \in BOOLEAN /\ postDisjoint \in BOOLEAN
NoStalePublication == ~bad
NoPartialVisibility == phase # "published" => visible = BaseIndex /\ watermark = 0
NoCompleteProgress == ~completeProgress
NoDisjointProgress == ~disjointProgress
=============================================================================
