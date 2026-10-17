------------- MODULE HawDBAdjacencyProjectionReadValidation -------------
EXTENDS Integers, FiniteSets
CONSTANTS MaxEpoch, SkipRoot, SkipEmptyAdjacency, SkipIncoming, FilterNeighbors,
          FilterRelationships, SkipCanonical, SkipLive, IgnoreBarriers, PrunePinned, ForgetRefusal
VARIABLES db, job, staleAccepted, disjointAccepted
vars == <<db, job, staleAccepted, disjointAccepted>>
Nodes == 1..4
Roots == {1, 2}
Edges == {1, 2}
Dirs == {"out", "in"}
Queries == {"point", "out", "in", "both", "filteredNode", "filteredEdge", "optional", "optionalNode", "optionalEdge"}
N(id) == <<"node", id, "">>
R(id) == <<"rel", id, "">>
A(id, dir) == <<"adj", id, dir>>
Barrier == <<"database", 0, "">>
Schema == <<"schema", 0, "">>
Keys == {N(id): id \in Nodes} \cup {R(id): id \in Edges}
         \cup {A(id, dir): id \in Roots, dir \in Dirs} \cup {Barrier, Schema}
Zero == [k \in Keys |-> 0]
Neighbor(e) == e + 2
Direction(e) == IF e = 1 THEN "out" ELSE "in"
ReadDirs(q) == CASE q = "point" -> {} [] q = "in" -> {"in"} [] q = "both" -> Dirs [] OTHER -> {"out"}
Scope(j) == {e \in Edges: j.root = 1 /\ j.edges[e].active /\ Direction(e) \in ReadDirs(j.query)}
Visible(j, e) == CASE j.query \in {"filteredNode", "optionalNode"} -> j.nodes[Neighbor(e)].label /\ j.nodes[Neighbor(e)].payload = 1
                     [] j.query \in {"filteredEdge", "optionalEdge"} -> j.edges[e].weight = 1 [] OTHER -> TRUE
Output(j) == IF j.query = "point" THEN {<<j.root, j.nodes[j.root].payload>>} ELSE
    IF j.query \in {"optional", "optionalNode", "optionalEdge"} /\ {e \in Scope(j): Visible(j,e)} = {} THEN {<<0, j.nodes[j.root].payload, 0, 0>>} ELSE
    {<<e, j.nodes[j.root].payload, j.nodes[Neighbor(e)].payload, j.edges[e].weight>>: e \in {s \in Scope(j): Visible(j,s)}}
ReferenceKeys(j) == {N(j.root)} \cup {A(j.root,d): d \in ReadDirs(j.query)}
    \cup UNION {{R(e), N(Neighbor(e))}: e \in Scope(j)}
ChosenKeys(j) == LET scanned == {e \in Scope(j): ~((SkipCanonical /\ j.cold /\ e = 1) \/ (SkipLive /\ (~j.cold \/ e = 2)))}
                        dirs == ReadDirs(j.query) \ (IF SkipIncoming THEN {"in"} ELSE {})
                    IN (IF SkipRoot THEN {} ELSE {N(j.root)})
                       \cup (IF SkipEmptyAdjacency /\ Scope(j) = {} THEN {} ELSE {A(j.root,d): d \in dirs})
                       \cup {R(e): e \in {s \in scanned: ~FilterRelationships \/ Visible(j,s)}}
                       \cup {N(Neighbor(e)): e \in {s \in scanned: ~FilterNeighbors \/ Visible(j,s)}}
IndexValid == /\ ~job.refused
    /\ \A k \in job.keys \cup (IF IgnoreBarriers THEN {} ELSE {Barrier,Schema}): db.stamps[k] <= job.epoch
OracleValid == /\ \A k \in ReferenceKeys(job) \cup {Barrier,Schema}: db.oracle[k] <= job.epoch
    /\ Output(job) = Output([job EXCEPT !.nodes = db.nodes, !.edges = db.edges])
Open == job.phase \in {"captured", "chosen", "read"}
InitialNodes == [id \in Nodes |-> [payload |-> 0, label |-> id = 3]]
InitialEdges == [e \in Edges |-> [active |-> e = 1, weight |-> 0]]
Init == /\ db \in {[epoch |-> 0, nodes |-> InitialNodes, edges |-> InitialEdges, cold |-> c, stamps |-> Zero, oracle |-> Zero]: c \in BOOLEAN}
        /\ job = [phase |-> "idle", epoch |-> 0, nodes |-> InitialNodes, edges |-> InitialEdges, cold |-> FALSE, root |-> 1, query |-> "point", keys |-> {}, refused |-> FALSE]
        /\ staleAccepted = FALSE /\ disjointAccepted = FALSE
Capture == /\ job.phase = "idle"
           /\ job' = [job EXCEPT !.phase = "captured", !.epoch = db.epoch, !.nodes = db.nodes, !.edges = db.edges, !.cold = db.cold]
           /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Choose(r,q) == /\ job.phase = "captured" /\ r \in Roots /\ q \in Queries
               /\ job' = [job EXCEPT !.phase = "chosen", !.root = r, !.query = q]
               /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Read(failed) == /\ job.phase = "chosen" /\ failed \in BOOLEAN
                /\ job' = [job EXCEPT !.phase = "read", !.keys = IF failed THEN {N(job.root)} ELSE ChosenKeys(job), !.refused = failed /\ ~ForgetRefusal]
                /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
NodeWrite(id,p,l,broad) == /\ db.epoch < MaxEpoch /\ id \in Nodes /\ p \in {0,1} /\ l \in BOOLEAN /\ broad \in BOOLEAN
    /\ LET next == db.epoch + 1
           reference == {N(id)} \cup (IF broad THEN {Barrier} ELSE {})
           optimized == IF broad THEN {Barrier} ELSE {N(id)}
       IN db' = [db EXCEPT !.epoch = next, !.nodes[id] = [payload |-> p, label |-> l],
          !.oracle = [k \in Keys |-> IF k \in reference THEN next ELSE db.oracle[k]],
          !.stamps = [k \in Keys |-> IF k \in optimized THEN next ELSE db.stamps[k]]]
    /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
EdgeWrite(e,a,w) == /\ db.epoch < MaxEpoch /\ e \in Edges /\ a \in BOOLEAN /\ w \in {0,1}
    /\ LET next == db.epoch + 1
           touched == {R(e)} \cup (IF a # db.edges[e].active THEN {A(1,Direction(e))} ELSE {})
       IN db' = [db EXCEPT !.epoch = next, !.edges[e] = [active |-> a, weight |-> w],
          !.oracle = [k \in Keys |-> IF k \in touched THEN next ELSE db.oracle[k]],
          !.stamps = [k \in Keys |-> IF k \in touched THEN next ELSE db.stamps[k]]]
    /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
SchemaWrite == /\ db.epoch < MaxEpoch
               /\ db' = [db EXCEPT !.epoch = @ + 1, !.oracle[Schema] = db.epoch + 1, !.stamps[Schema] = db.epoch + 1]
               /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
Prune(k) == /\ k \in Keys /\ db.stamps[k] # 0 /\ (~Open \/ db.stamps[k] <= job.epoch \/ PrunePinned)
            /\ db' = [db EXCEPT !.stamps[k] = 0]
            /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
Accept == /\ job.phase = "read" /\ IndexValid
          /\ job' = [job EXCEPT !.phase = "done"]
          /\ staleAccepted' = (staleAccepted \/ ~OracleValid)
          /\ disjointAccepted' = (disjointAccepted \/ (db.epoch > job.epoch /\ OracleValid))
          /\ UNCHANGED db
Next == Capture \/ (\E r \in Roots,q \in Queries: Choose(r,q)) \/ (\E f \in BOOLEAN: Read(f))
    \/ (\E id \in Nodes,p \in {0,1},l \in BOOLEAN,b \in BOOLEAN: NodeWrite(id,p,l,b))
    \/ (\E e \in Edges,a \in BOOLEAN,w \in {0,1}: EdgeWrite(e,a,w)) \/ SchemaWrite \/ (\E k \in Keys: Prune(k)) \/ Accept
TypeOK == /\ db.epoch \in 0..MaxEpoch /\ db.nodes \in [Nodes -> [payload: {0,1}, label: BOOLEAN]]
    /\ db.edges \in [Edges -> [active: BOOLEAN, weight: {0,1}]] /\ db.cold \in BOOLEAN
    /\ db.oracle \in [Keys -> 0..MaxEpoch] /\ db.stamps \in [Keys -> 0..MaxEpoch]
    /\ job.phase \in {"idle","captured","chosen","read","done"} /\ job.epoch \in 0..MaxEpoch
    /\ job.root \in Roots /\ job.query \in Queries /\ job.keys \subseteq Keys /\ job.refused \in BOOLEAN
    /\ staleAccepted \in BOOLEAN /\ disjointAccepted \in BOOLEAN
NoStaleAcceptance == ~staleAccepted
NoDisjointAcceptance == ~disjointAccepted
=============================================================================
