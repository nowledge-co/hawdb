-------------------- MODULE HawDBRelationalIndexDemandRead --------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A generation-pinned relational candidate reader opens with no resident  *)
(* pages. SQL selection is owned by a separate activation boundary.         *)
(* One lookup loads an ordered root/interior/leaf/posting path on demand,    *)
(* applies independent page/byte/row limits, and emits only an ordered      *)
(* prefix of the materialized-index oracle.                                 *)
(***************************************************************************)

Pages == 1..3
Rows == 1..4
TargetPages == <<1, 2, 3>>
OracleRows == <<1, 2, 3, 4>>
ReadStates == {"closed", "open", "reading", "succeeded", "failed"}

RowsForPage(page) ==
    CASE page = 1 -> <<>>
      [] page = 2 -> <<1, 2>>
      [] OTHER -> <<3, 4>>

IsPrefix(prefix, complete) ==
    /\ Len(prefix) <= Len(complete)
    /\ \A index \in 1..Len(prefix): prefix[index] = complete[index]

VARIABLES
    readState,
    handleOpen,
    lookupStarted,
    pageBudget,
    byteBudget,
    rowBudget,
    pageCursor,
    pageLoaded,
    rowCursor,
    loadedPages,
    pagesRead,
    bytesRead,
    emittedRows,
    stoppedEarly,
    corruptPages,
    poisoned,
    sqlUsesShadow

vars == <<
    readState,
    handleOpen,
    lookupStarted,
    pageBudget,
    byteBudget,
    rowBudget,
    pageCursor,
    pageLoaded,
    rowCursor,
    loadedPages,
    pagesRead,
    bytesRead,
    emittedRows,
    stoppedEarly,
    corruptPages,
    poisoned,
    sqlUsesShadow
>>

Init ==
    /\ readState = "closed"
    /\ handleOpen = FALSE
    /\ lookupStarted = FALSE
    /\ pageBudget = 1
    /\ byteBudget = 1
    /\ rowBudget = 1
    /\ pageCursor = 1
    /\ pageLoaded = FALSE
    /\ rowCursor = 1
    /\ loadedPages = {}
    /\ pagesRead = 0
    /\ bytesRead = 0
    /\ emittedRows = <<>>
    /\ stoppedEarly = FALSE
    /\ corruptPages = {}
    /\ poisoned = FALSE
    /\ sqlUsesShadow = FALSE

OpenCold ==
    /\ readState = "closed"
    /\ readState' = "open"
    /\ handleOpen' = TRUE
    /\ lookupStarted' = FALSE
    /\ pageCursor' = 1
    /\ pageLoaded' = FALSE
    /\ rowCursor' = 1
    /\ loadedPages' = {}
    /\ pagesRead' = 0
    /\ bytesRead' = 0
    /\ emittedRows' = <<>>
    /\ stoppedEarly' = FALSE
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        pageBudget,
        byteBudget,
        rowBudget,
        corruptPages,
        sqlUsesShadow
        >>

BeginLookup ==
    /\ readState = "open"
    /\ \E pages \in 1..Len(TargetPages): pageBudget' = pages
    /\ \E bytes \in 1..Len(TargetPages): byteBudget' = bytes
    /\ \E rows \in 1..Len(OracleRows): rowBudget' = rows
    /\ readState' = "reading"
    /\ lookupStarted' = TRUE
    /\ pageCursor' = 1
    /\ pageLoaded' = FALSE
    /\ rowCursor' = 1
    /\ loadedPages' = {}
    /\ pagesRead' = 0
    /\ bytesRead' = 0
    /\ emittedRows' = <<>>
    /\ stoppedEarly' = FALSE
    /\ UNCHANGED <<
        handleOpen,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

LoadHealthyPage ==
    /\ readState = "reading"
    /\ ~pageLoaded
    /\ pageCursor <= Len(TargetPages)
    /\ TargetPages[pageCursor] \notin corruptPages
    /\ pagesRead < pageBudget
    /\ bytesRead + 1 <= byteBudget
    /\ pageLoaded' = TRUE
    /\ rowCursor' = 1
    /\ loadedPages' = loadedPages \cup {TargetPages[pageCursor]}
    /\ pagesRead' = pagesRead + 1
    /\ bytesRead' = bytesRead + 1
    /\ UNCHANGED <<
        readState,
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        emittedRows,
        stoppedEarly,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

LoadCorruptPage ==
    /\ readState = "reading"
    /\ ~pageLoaded
    /\ pageCursor <= Len(TargetPages)
    /\ TargetPages[pageCursor] \in corruptPages
    /\ pagesRead < pageBudget
    /\ bytesRead + 1 <= byteBudget
    /\ readState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        stoppedEarly,
        corruptPages,
        sqlUsesShadow
        >>

RejectPageBudget ==
    /\ readState = "reading"
    /\ ~pageLoaded
    /\ pageCursor <= Len(TargetPages)
    /\ \/ pagesRead >= pageBudget
       \/ bytesRead + 1 > byteBudget
    /\ readState' = "failed"
    /\ UNCHANGED <<
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        stoppedEarly,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

EmitRow ==
    /\ readState = "reading"
    /\ pageLoaded
    /\ rowCursor <= Len(RowsForPage(TargetPages[pageCursor]))
    /\ Len(emittedRows) < rowBudget
    /\ emittedRows' = Append(
        emittedRows,
        RowsForPage(TargetPages[pageCursor])[rowCursor]
        )
    /\ rowCursor' = rowCursor + 1
    /\ UNCHANGED <<
        readState,
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        loadedPages,
        pagesRead,
        bytesRead,
        stoppedEarly,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

RejectRowBudget ==
    /\ readState = "reading"
    /\ pageLoaded
    /\ rowCursor <= Len(RowsForPage(TargetPages[pageCursor]))
    /\ Len(emittedRows) >= rowBudget
    /\ readState' = "failed"
    /\ UNCHANGED <<
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        stoppedEarly,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

FinishPage ==
    /\ readState = "reading"
    /\ pageLoaded
    /\ rowCursor > Len(RowsForPage(TargetPages[pageCursor]))
    /\ pageCursor' = pageCursor + 1
    /\ pageLoaded' = FALSE
    /\ rowCursor' = 1
    /\ UNCHANGED <<
        readState,
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        stoppedEarly,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

CompleteLookup ==
    /\ readState = "reading"
    /\ ~pageLoaded
    /\ pageCursor > Len(TargetPages)
    /\ readState' = "succeeded"
    /\ stoppedEarly' = FALSE
    /\ UNCHANGED <<
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

StopEarly ==
    /\ readState = "reading"
    /\ Len(emittedRows) > 0
    /\ readState' = "succeeded"
    /\ stoppedEarly' = TRUE
    /\ UNCHANGED <<
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        corruptPages,
        poisoned,
        sqlUsesShadow
        >>

CorruptColdPage ==
    /\ \E page \in Pages \ loadedPages:
        corruptPages' = corruptPages \cup {page}
    /\ UNCHANGED <<
        readState,
        handleOpen,
        lookupStarted,
        pageBudget,
        byteBudget,
        rowBudget,
        pageCursor,
        pageLoaded,
        rowCursor,
        loadedPages,
        pagesRead,
        bytesRead,
        emittedRows,
        stoppedEarly,
        poisoned,
        sqlUsesShadow
        >>

CloseReader ==
    /\ handleOpen
    /\ readState \in {"open", "succeeded", "failed"}
    /\ readState' = "closed"
    /\ handleOpen' = FALSE
    /\ lookupStarted' = FALSE
    /\ pageCursor' = 1
    /\ pageLoaded' = FALSE
    /\ rowCursor' = 1
    /\ loadedPages' = {}
    /\ pagesRead' = 0
    /\ bytesRead' = 0
    /\ emittedRows' = <<>>
    /\ stoppedEarly' = FALSE
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        pageBudget,
        byteBudget,
        rowBudget,
        corruptPages,
        sqlUsesShadow
        >>

Next ==
    \/ OpenCold
    \/ BeginLookup
    \/ LoadHealthyPage
    \/ LoadCorruptPage
    \/ RejectPageBudget
    \/ EmitRow
    \/ RejectRowBudget
    \/ FinishPage
    \/ CompleteLookup
    \/ StopEarly
    \/ CorruptColdPage
    \/ CloseReader

TypeOK ==
    /\ readState \in ReadStates
    /\ handleOpen \in BOOLEAN
    /\ lookupStarted \in BOOLEAN
    /\ pageBudget \in 1..Len(TargetPages)
    /\ byteBudget \in 1..Len(TargetPages)
    /\ rowBudget \in 1..Len(OracleRows)
    /\ pageCursor \in 1..(Len(TargetPages) + 1)
    /\ pageLoaded \in BOOLEAN
    /\ rowCursor \in 1..3
    /\ loadedPages \subseteq Pages
    /\ pagesRead \in 0..Len(TargetPages)
    /\ bytesRead \in 0..Len(TargetPages)
    /\ emittedRows \in Seq(Rows)
    /\ stoppedEarly \in BOOLEAN
    /\ corruptPages \subseteq Pages
    /\ poisoned \in BOOLEAN
    /\ sqlUsesShadow \in BOOLEAN

ColdOpenDoesNotLoadPages ==
    readState = "open" => /\ loadedPages = {} /\ ~lookupStarted

ReadBudgetsNeverExceeded ==
    /\ pagesRead <= pageBudget
    /\ bytesRead <= byteBudget
    /\ Len(emittedRows) <= rowBudget

LoadsOnlyDemandPath ==
    loadedPages = {TargetPages[index] : index \in 1..pagesRead}

EmittedRowsStayOraclePrefix == IsPrefix(emittedRows, OracleRows)

FullSuccessMatchesOracle ==
    readState = "succeeded" /\ ~stoppedEarly => emittedRows = OracleRows

CorruptionPoisonsOnlyReader ==
    poisoned => /\ handleOpen /\ readState = "failed"

ProductionSqlRemainsOnOracle == ~sqlUsesShadow

Spec == Init /\ [][Next]_vars

=============================================================================
