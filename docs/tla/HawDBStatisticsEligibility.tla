-------------------- MODULE HawDBStatisticsEligibility --------------------
EXTENDS FiniteSets

(***************************************************************************)
(* Property statistics admit compact scalar and VARCHAR groups. Declared    *)
(* TEXT and large groups are excluded before payload facts are created. A   *)
(* mixed group may produce an early compact fact, but a later unsupported    *)
(* observation fences the complete group out of the published state.        *)
(***************************************************************************)

CompactProperty == "compact"
VarcharProperty == "varchar"
TextProperty == "text"
LargeProperty == "large"
MixedProperty == "mixed"

Properties == {
    CompactProperty,
    VarcharProperty,
    TextProperty,
    LargeProperty,
    MixedProperty
}

Observations == {
    [property |-> CompactProperty, kind |-> "compact"],
    [property |-> VarcharProperty, kind |-> "compact"],
    [property |-> TextProperty, kind |-> "unsupported"],
    [property |-> LargeProperty, kind |-> "unsupported"],
    [property |-> MixedProperty, kind |-> "compact"],
    [property |-> MixedProperty, kind |-> "unsupported"]
}

VARIABLES phase, remaining, facts, excluded, published

vars == <<phase, remaining, facts, excluded, published>>

Init ==
    /\ phase = "collecting"
    /\ remaining = Observations
    /\ facts = {}
    /\ excluded = {}
    /\ published = {}

ObserveCompact ==
    /\ phase = "collecting"
    /\ \E observation \in remaining:
        /\ observation.kind = "compact"
        /\ remaining' = remaining \ {observation}
        /\ facts' = IF observation.property \in excluded
                     THEN facts
                     ELSE facts \cup {observation.property}
    /\ UNCHANGED <<phase, excluded, published>>

ObserveUnsupported ==
    /\ phase = "collecting"
    /\ \E observation \in remaining:
        /\ observation.kind = "unsupported"
        /\ remaining' = remaining \ {observation}
        /\ excluded' = excluded \cup {observation.property}
    /\ UNCHANGED <<phase, facts, published>>

Observe == ObserveCompact \/ ObserveUnsupported

Publish ==
    /\ phase = "collecting"
    /\ remaining = {}
    /\ phase' = "published"
    /\ published' = facts \ excluded
    /\ UNCHANGED <<remaining, facts, excluded>>

Next == Observe \/ Publish

TypeOK ==
    /\ phase \in {"collecting", "published"}
    /\ remaining \subseteq Observations
    /\ facts \subseteq Properties
    /\ excluded \subseteq Properties
    /\ published \subseteq Properties

NoPartialPublication == published \subseteq facts \ excluded

UnsupportedGroupsAreNeverPublished ==
    published \cap {TextProperty, LargeProperty, MixedProperty} = {}

NoEarlyPublication == phase = "collecting" => published = {}

CompletedMixedScanIsExcluded ==
    phase = "published" => MixedProperty \in excluded

CompletedVarcharScanIsPublished ==
    phase = "published" => VarcharProperty \in published

EventuallyPublishes == phase = "collecting" ~> phase = "published"

Spec == Init /\ [][Next]_vars /\ WF_vars(Observe) /\ WF_vars(Publish)

=============================================================================
