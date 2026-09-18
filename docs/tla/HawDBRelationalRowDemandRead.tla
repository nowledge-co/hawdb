--------------------- MODULE HawDBRelationalRowDemandRead ---------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A relational row demand reader pins one exact row/overflow root pair.   *)
(* It opens with every row page cold, loads at most one page at a time,     *)
(* decodes an ordered row prefix, and hydrates only requested overflow      *)
(* fields. The corrupt-page set also abstracts a page whose encoded column   *)
(* count differs from the count bound by its table root. Admission,          *)
(* cancellation, and callback panic release the current page without        *)
(* poisoning; page, root-shape, or overflow corruption poisons the reader.   *)
(***************************************************************************)

Pages == 1..2
Rows == 1..3
Fields == {1, 2}
OverflowFields == {2}
OracleRows == <<1, 2, 3>>
ReadStates == {"open", "admitting", "reading", "succeeded", "failed", "stopped"}
Outcomes == {"none", "success", "admission", "corruption", "cancel", "panic"}
TerminalStates == {"succeeded", "failed", "stopped"}

RowsForPage(page) ==
    CASE page = 1 -> <<1, 2>>
      [] OTHER -> <<3>>

IsPrefix(prefix, complete) ==
    /\ Len(prefix) <= Len(complete)
    /\ \A ordinal \in 1..Len(prefix): prefix[ordinal] = complete[ordinal]

RequestedOverflow(requested) == requested \intersect OverflowFields

VARIABLES
    readState,
    outcome,
    rootGeneration,
    requestedFields,
    pageBudget,
    byteBudget,
    rowBudget,
    treeHeightBudget,
    hydrationBudget,
    pageCursor,
    rowCursor,
    pagesRead,
    bytesRead,
    emittedRows,
    hydratedPairs,
    hydrationCount,
    residentPages,
    pinnedPage,
    corruptPages,
    corruptOverflow,
    poisoned,
    stoppedEarly

vars == <<
    readState,
    outcome,
    rootGeneration,
    requestedFields,
    pageBudget,
    byteBudget,
    rowBudget,
    treeHeightBudget,
    hydrationBudget,
    pageCursor,
    rowCursor,
    pagesRead,
    bytesRead,
    emittedRows,
    hydratedPairs,
    hydrationCount,
    residentPages,
    pinnedPage,
    corruptPages,
    corruptOverflow,
    poisoned,
    stoppedEarly
>>

Init ==
    /\ readState = "open"
    /\ outcome = "none"
    /\ rootGeneration = 1
    /\ requestedFields = {}
    /\ pageBudget = 1
    /\ byteBudget = 1
    /\ rowBudget = 1
    /\ treeHeightBudget = 1
    /\ hydrationBudget = 0
    /\ pageCursor = 1
    /\ rowCursor = 1
    /\ pagesRead = 0
    /\ bytesRead = 0
    /\ emittedRows = <<>>
    /\ hydratedPairs = {}
    /\ hydrationCount = 0
    /\ residentPages = {}
    /\ pinnedPage = 0
    /\ corruptPages = {}
    /\ corruptOverflow = FALSE
    /\ poisoned = FALSE
    /\ stoppedEarly = FALSE

BeginRead ==
    /\ readState = "open"
    /\ \E fields \in SUBSET Fields:
       \E pages \in 1..2:
       \E bytes \in 1..2:
       \E rows \in 1..3:
       \E height \in 1..2:
       \E hydration \in 0..3:
       \E badPages \in SUBSET Pages:
       \E badOverflow \in BOOLEAN:
            /\ requestedFields' = fields
            /\ pageBudget' = pages
            /\ byteBudget' = bytes
            /\ rowBudget' = rows
            /\ treeHeightBudget' = height
            /\ hydrationBudget' = hydration
            /\ corruptPages' = badPages
            /\ corruptOverflow' = badOverflow
    /\ readState' = "admitting"
    /\ outcome' = "none"
    /\ pageCursor' = 1
    /\ rowCursor' = 1
    /\ pagesRead' = 0
    /\ bytesRead' = 0
    /\ emittedRows' = <<>>
    /\ hydratedPairs' = {}
    /\ hydrationCount' = 0
    /\ residentPages' = {}
    /\ pinnedPage' = 0
    /\ poisoned' = FALSE
    /\ stoppedEarly' = FALSE
    /\ UNCHANGED rootGeneration

AdmitDescriptorSearch ==
    /\ readState = "admitting"
    /\ treeHeightBudget >= 2
    /\ readState' = "reading"
    /\ UNCHANGED <<
        outcome,
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        pinnedPage,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

RejectDescriptorSearch ==
    /\ readState = "admitting"
    /\ treeHeightBudget < 2
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ pinnedPage' = 0
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

LoadHealthyPage ==
    /\ readState = "reading"
    /\ pinnedPage = 0
    /\ pageCursor \in Pages
    /\ pageCursor \notin corruptPages
    /\ pagesRead < pageBudget
    /\ bytesRead < byteBudget
    /\ pinnedPage' = pageCursor
    /\ residentPages' = {pageCursor}
    /\ pagesRead' = pagesRead + 1
    /\ bytesRead' = bytesRead + 1
    /\ rowCursor' = 1
    /\ UNCHANGED <<
        readState,
        outcome,
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

RejectPageBudget ==
    /\ readState = "reading"
    /\ pinnedPage = 0
    /\ pageCursor \in Pages
    /\ \/ pagesRead >= pageBudget
       \/ bytesRead >= byteBudget
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        pinnedPage,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

LoadCorruptPage ==
    /\ readState = "reading"
    /\ pinnedPage = 0
    /\ pageCursor \in Pages
    /\ pageCursor \in corruptPages
    /\ pagesRead < pageBudget
    /\ bytesRead < byteBudget
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ pinnedPage' = 0
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        stoppedEarly
        >>

EmitRow ==
    /\ readState = "reading"
    /\ pinnedPage = pageCursor
    /\ rowCursor <= Len(RowsForPage(pageCursor))
    /\ Len(emittedRows) < rowBudget
    /\ LET row == RowsForPage(pageCursor)[rowCursor]
           fields == RequestedOverflow(requestedFields)
       IN /\ ~corruptOverflow \/ fields = {}
          /\ hydrationCount + Cardinality(fields) <= hydrationBudget
          /\ emittedRows' = Append(emittedRows, row)
          /\ hydratedPairs' = hydratedPairs \cup {<<row, field>> : field \in fields}
          /\ hydrationCount' = hydrationCount + Cardinality(fields)
    /\ rowCursor' = rowCursor + 1
    /\ UNCHANGED <<
        readState,
        outcome,
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        pagesRead,
        bytesRead,
        residentPages,
        pinnedPage,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

RejectRowBudget ==
    /\ readState = "reading"
    /\ pinnedPage = pageCursor
    /\ rowCursor <= Len(RowsForPage(pageCursor))
    /\ Len(emittedRows) >= rowBudget
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ pinnedPage' = 0
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

RejectHydrationBudget ==
    /\ readState = "reading"
    /\ pinnedPage = pageCursor
    /\ rowCursor <= Len(RowsForPage(pageCursor))
    /\ Len(emittedRows) < rowBudget
    /\ hydrationCount + Cardinality(RequestedOverflow(requestedFields)) > hydrationBudget
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ pinnedPage' = 0
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

LoadCorruptOverflow ==
    /\ readState = "reading"
    /\ pinnedPage = pageCursor
    /\ rowCursor <= Len(RowsForPage(pageCursor))
    /\ Len(emittedRows) < rowBudget
    /\ RequestedOverflow(requestedFields) /= {}
    /\ corruptOverflow
    /\ hydrationCount + Cardinality(RequestedOverflow(requestedFields)) <= hydrationBudget
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ pinnedPage' = 0
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        stoppedEarly
        >>

ReleasePage ==
    /\ readState = "reading"
    /\ pinnedPage = pageCursor
    /\ rowCursor > Len(RowsForPage(pageCursor))
    /\ pinnedPage' = 0
    /\ pageCursor' = pageCursor + 1
    /\ rowCursor' = 1
    /\ UNCHANGED <<
        readState,
        outcome,
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

FinishRead ==
    /\ readState = "reading"
    /\ pinnedPage = 0
    /\ pageCursor > Cardinality(Pages)
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        pinnedPage,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

StopEarly ==
    /\ readState = "reading"
    /\ pinnedPage /= 0
    /\ Len(emittedRows) > 0
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ pinnedPage' = 0
    /\ stoppedEarly' = TRUE
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned
        >>

CancelRead ==
    /\ readState \in {"admitting", "reading"}
    /\ readState' = "stopped"
    /\ outcome' = "cancel"
    /\ pinnedPage' = 0
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

CallbackPanics ==
    /\ readState = "reading"
    /\ pinnedPage /= 0
    /\ Len(emittedRows) > 0
    /\ readState' = "stopped"
    /\ outcome' = "panic"
    /\ pinnedPage' = 0
    /\ UNCHANGED <<
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        residentPages,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

EvictUnpinned ==
    /\ residentPages /= {}
    /\ pinnedPage = 0
    /\ residentPages' = {}
    /\ UNCHANGED <<
        readState,
        outcome,
        rootGeneration,
        requestedFields,
        pageBudget,
        byteBudget,
        rowBudget,
        treeHeightBudget,
        hydrationBudget,
        pageCursor,
        rowCursor,
        pagesRead,
        bytesRead,
        emittedRows,
        hydratedPairs,
        hydrationCount,
        pinnedPage,
        corruptPages,
        corruptOverflow,
        poisoned,
        stoppedEarly
        >>

Next ==
    \/ BeginRead
    \/ AdmitDescriptorSearch
    \/ RejectDescriptorSearch
    \/ LoadHealthyPage
    \/ RejectPageBudget
    \/ LoadCorruptPage
    \/ EmitRow
    \/ RejectRowBudget
    \/ RejectHydrationBudget
    \/ LoadCorruptOverflow
    \/ ReleasePage
    \/ FinishRead
    \/ StopEarly
    \/ CancelRead
    \/ CallbackPanics
    \/ EvictUnpinned

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ readState \in ReadStates
    /\ outcome \in Outcomes
    /\ rootGeneration = 1
    /\ requestedFields \subseteq Fields
    /\ pageBudget \in 1..2
    /\ byteBudget \in 1..2
    /\ rowBudget \in 1..3
    /\ treeHeightBudget \in 1..2
    /\ hydrationBudget \in 0..3
    /\ pageCursor \in 1..3
    /\ rowCursor \in 1..3
    /\ pagesRead \in 0..2
    /\ bytesRead \in 0..2
    /\ emittedRows \in Seq(Rows)
    /\ hydratedPairs \subseteq (Rows \X Fields)
    /\ hydrationCount \in 0..3
    /\ residentPages \subseteq Pages
    /\ Cardinality(residentPages) <= 1
    /\ pinnedPage \in 0..2
    /\ corruptPages \subseteq Pages
    /\ corruptOverflow \in BOOLEAN
    /\ poisoned \in BOOLEAN
    /\ stoppedEarly \in BOOLEAN

RootGenerationStaysPinned == rootGeneration = 1

ReadBudgetsNeverExceeded ==
    /\ pagesRead <= pageBudget
    /\ bytesRead <= byteBudget
    /\ Len(emittedRows) <= rowBudget
    /\ hydrationCount <= hydrationBudget

AtMostOnePageIsPinned ==
    /\ pinnedPage \in 0..2
    /\ pinnedPage /= 0 => pinnedPage \in residentPages

TerminalStatesReleasePins == readState \in TerminalStates => pinnedPage = 0

EmittedRowsStayOrdered == IsPrefix(emittedRows, OracleRows)

HydratesOnlyRequestedOverflow ==
    \A pair \in hydratedPairs:
        /\ pair[1] \in Rows
        /\ pair[2] \in requestedFields
        /\ pair[2] \in OverflowFields

EveryEmittedOverflowFieldIsHydrated ==
    \A row \in {emittedRows[index] : index \in 1..Len(emittedRows)}:
        \A field \in RequestedOverflow(requestedFields):
            <<row, field>> \in hydratedPairs

FullSuccessMatchesOracle ==
    (readState = "succeeded" /\ ~stoppedEarly) => emittedRows = OracleRows

OnlyCorruptionPoisons == poisoned <=> outcome = "corruption"

=============================================================================
