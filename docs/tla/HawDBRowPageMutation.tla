------------------------ MODULE HawDBRowPageMutation ------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A table root persists the next never-issued logical PageId. Point       *)
(* mutation reads at most one base leaf, preserves that leaf's PageId for  *)
(* the first COW output, allocates monotonically for split pages, removes  *)
(* empty leaves without recycling ids, and never changes a pinned base     *)
(* root. An empty-table bootstrap emits ordered pages while retaining at   *)
(* most one page plus one candidate row.                                   *)
(***************************************************************************)

CONSTANT MaxPageId, PageCapacity

ASSUME /\ MaxPageId = 4
       /\ PageCapacity = 2

Keys == 1..3
PageIds == 1..MaxPageId
Scenarios == {"insertSplit", "update", "delete", "deleteThenSplit", "noop", "bootstrap"}
Phases == {"base", "deleted", "planned"}

BaseRoot(scenario) ==
    IF scenario = "bootstrap" THEN <<>> ELSE <<1, 2>>

BaseRows(scenario) ==
    IF scenario = "bootstrap"
    THEN [page \in PageIds |-> {}]
    ELSE IF scenario = "deleteThenSplit"
    THEN [page \in PageIds |->
        CASE page = 1 -> {1}
          [] page = 2 -> {2, 3}
          [] OTHER -> {}]
    ELSE [page \in PageIds |->
        CASE page = 1 -> {1}
          [] page = 2 -> {3}
          [] OTHER -> {}]

BaseNextPageId(scenario) ==
    IF scenario = "bootstrap" THEN 1 ELSE 3

ExpectedRows(scenario) ==
    CASE scenario = "insertSplit" -> {1, 2, 3}
      [] scenario = "delete" -> {3}
      [] scenario = "deleteThenSplit" -> {1, 2, 3}
      [] scenario = "bootstrap" -> {1, 2, 3}
      [] OTHER -> {1, 3}

RootSet(root) == {root[index] : index \in 1..Len(root)}

RootRows(root, rows) ==
    UNION {rows[page] : page \in RootSet(root)}

VARIABLES
    scenario,
    phase,
    root,
    rows,
    nextPageId,
    dirtyPages,
    deletedPages,
    pagesRead,
    peakBufferedRows,
    pinnedRoot,
    pinnedRows,
    pinnedNextPageId

vars == <<
    scenario,
    phase,
    root,
    rows,
    nextPageId,
    dirtyPages,
    deletedPages,
    pagesRead,
    peakBufferedRows,
    pinnedRoot,
    pinnedRows,
    pinnedNextPageId
>>

Init ==
    /\ scenario \in Scenarios
    /\ phase = "base"
    /\ root = BaseRoot(scenario)
    /\ rows = BaseRows(scenario)
    /\ nextPageId = BaseNextPageId(scenario)
    /\ dirtyPages = {}
    /\ deletedPages = {}
    /\ pagesRead = 0
    /\ peakBufferedRows = 0
    /\ pinnedRoot = BaseRoot(scenario)
    /\ pinnedRows = BaseRows(scenario)
    /\ pinnedNextPageId = BaseNextPageId(scenario)

PlanInsertSplit ==
    /\ phase = "base"
    /\ scenario = "insertSplit"
    /\ phase' = "planned"
    /\ root' = <<1, 2, 3>>
    /\ rows' = [rows EXCEPT ![2] = {2}, ![3] = {3}]
    /\ nextPageId' = 4
    /\ dirtyPages' = {2, 3}
    /\ deletedPages' = {}
    /\ pagesRead' = 1
    /\ peakBufferedRows' = 0
    /\ UNCHANGED <<
        scenario, pinnedRoot, pinnedRows, pinnedNextPageId
        >>

PlanUpdate ==
    /\ phase = "base"
    /\ scenario = "update"
    /\ phase' = "planned"
    /\ dirtyPages' = {1}
    /\ pagesRead' = 1
    /\ UNCHANGED <<
        scenario, root, rows, nextPageId, deletedPages, peakBufferedRows,
        pinnedRoot, pinnedRows, pinnedNextPageId
        >>

PlanDelete ==
    /\ phase = "base"
    /\ scenario = "delete"
    /\ phase' = "planned"
    /\ root' = <<2>>
    /\ rows' = [rows EXCEPT ![1] = {}]
    /\ dirtyPages' = {}
    /\ deletedPages' = {1}
    /\ pagesRead' = 1
    /\ UNCHANGED <<
        scenario, nextPageId, peakBufferedRows, pinnedRoot, pinnedRows,
        pinnedNextPageId
        >>

DeleteBeforeSplit ==
    /\ phase = "base"
    /\ scenario = "deleteThenSplit"
    /\ phase' = "deleted"
    /\ root' = <<2>>
    /\ rows' = [rows EXCEPT ![1] = {}]
    /\ dirtyPages' = {}
    /\ deletedPages' = {1}
    /\ pagesRead' = 1
    /\ UNCHANGED <<
        scenario, nextPageId, peakBufferedRows, pinnedRoot, pinnedRows,
        pinnedNextPageId
        >>

InsertAfterDelete ==
    /\ phase = "deleted"
    /\ scenario = "deleteThenSplit"
    /\ phase' = "planned"
    /\ root' = <<2, 3>>
    /\ rows' = [rows EXCEPT ![2] = {1, 2}, ![3] = {3}]
    /\ nextPageId' = 4
    /\ dirtyPages' = {2, 3}
    /\ pagesRead' = 2
    /\ UNCHANGED <<
        scenario, deletedPages, peakBufferedRows, pinnedRoot, pinnedRows,
        pinnedNextPageId
        >>

PlanNoop ==
    /\ phase = "base"
    /\ scenario = "noop"
    /\ phase' = "planned"
    /\ pagesRead' = 1
    /\ UNCHANGED <<
        scenario, root, rows, nextPageId, dirtyPages, deletedPages,
        peakBufferedRows, pinnedRoot, pinnedRows, pinnedNextPageId
        >>

PlanBootstrap ==
    /\ phase = "base"
    /\ scenario = "bootstrap"
    /\ phase' = "planned"
    /\ root' = <<1, 2>>
    /\ rows' = [rows EXCEPT ![1] = {1, 2}, ![2] = {3}]
    /\ nextPageId' = 3
    /\ dirtyPages' = {1, 2}
    /\ deletedPages' = {}
    /\ pagesRead' = 0
    /\ peakBufferedRows' = 3
    /\ UNCHANGED <<
        scenario, pinnedRoot, pinnedRows, pinnedNextPageId
        >>

Next ==
    \/ PlanInsertSplit
    \/ PlanUpdate
    \/ PlanDelete
    \/ DeleteBeforeSplit
    \/ InsertAfterDelete
    \/ PlanNoop
    \/ PlanBootstrap

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ scenario \in Scenarios
    /\ phase \in Phases
    /\ root \in Seq(PageIds)
    /\ rows \in [PageIds -> SUBSET Keys]
    /\ nextPageId \in 1..MaxPageId
    /\ dirtyPages \subseteq PageIds
    /\ deletedPages \subseteq PageIds
    /\ pagesRead \in Nat
    /\ peakBufferedRows \in Nat
    /\ pinnedRoot \in Seq(PageIds)
    /\ pinnedRows \in [PageIds -> SUBSET Keys]
    /\ pinnedNextPageId \in 1..MaxPageId

RootPageIdsAreUnique ==
    Len(root) = Cardinality(RootSet(root))

ActivePageIdsWereAllocated ==
    \A page \in RootSet(root): page < nextPageId

AllocatorNeverMovesBackward ==
    nextPageId >= BaseNextPageId(scenario)

AllocatorNeverReusesBaseIds ==
    {page \in BaseNextPageId(scenario)..(nextPageId - 1):
        page \in RootSet(BaseRoot(scenario))} = {}

RootContainsNoEmptyPages ==
    \A page \in RootSet(root): rows[page] # {}

DirtyAndDeletedPagesAreSound ==
    /\ dirtyPages \subseteq RootSet(root)
    /\ deletedPages \cap RootSet(root) = {}
    /\ deletedPages \subseteq RootSet(BaseRoot(scenario))

PlannedRowsMatchMutation ==
    phase = "planned" => RootRows(root, rows) = ExpectedRows(scenario)

PointMutationReadsOneLeaf ==
    IF scenario = "deleteThenSplit"
    THEN pagesRead <= 2
    ELSE pagesRead <= 1

BootstrapReadsNoBaseAndIsBounded ==
    scenario = "bootstrap" =>
        /\ pagesRead = 0
        /\ peakBufferedRows <= PageCapacity + 1

SplitPreservesLeftIdAndAllocatesRight ==
    /\ phase = "planned"
    /\ scenario = "insertSplit"
    => /\ root = <<1, 2, 3>>
       /\ 2 \in RootSet(BaseRoot(scenario))
       /\ 3 \notin RootSet(BaseRoot(scenario))

DeletedPageIdIsNeverReused ==
    /\ phase = "planned"
    /\ scenario = "deleteThenSplit"
    => /\ 1 \notin RootSet(root)
       /\ root = <<2, 3>>
       /\ nextPageId = 4

PinnedBaseDoesNotDrift ==
    /\ pinnedRoot = BaseRoot(scenario)
    /\ pinnedRows = BaseRows(scenario)
    /\ pinnedNextPageId = BaseNextPageId(scenario)

=============================================================================
