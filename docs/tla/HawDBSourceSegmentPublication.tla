-------------------- MODULE HawDBSourceSegmentPublication -------------------
EXTENDS Integers, Naturals

CONSTANT Readers, MaxEpoch

VARIABLES graphEpoch,
          manifestEpoch,
          durableSegmentEpochs,
          artifactEpoch,
          artifactState,
          buildEpoch,
          builder,
          readerEpoch,
          readerPath

vars == <<graphEpoch,
          manifestEpoch,
          durableSegmentEpochs,
          artifactEpoch,
          artifactState,
          buildEpoch,
          builder,
          readerEpoch,
          readerPath>>

ArtifactStates == {"available", "mixed", "missing", "corrupt"}
BuilderStates == {"none", "staged", "durable", "payload_replaced", "sidecar_replaced"}
NoActiveReaders == \A reader \in Readers: readerEpoch[reader] = -1

Init ==
    /\ graphEpoch = 0
    /\ manifestEpoch = 0
    /\ durableSegmentEpochs = {0}
    /\ artifactEpoch = 0
    /\ artifactState = "available"
    /\ buildEpoch = 0
    /\ builder = "none"
    /\ readerEpoch = [reader \in Readers |-> -1]
    /\ readerPath = [reader \in Readers |-> "none"]

CommitGraph ==
    /\ builder = "none"
    /\ graphEpoch < MaxEpoch
    /\ graphEpoch' = graphEpoch + 1
    /\ UNCHANGED <<manifestEpoch, durableSegmentEpochs, artifactEpoch,
                    artifactState, buildEpoch, builder, readerEpoch, readerPath>>

BeginBuild ==
    /\ builder = "none"
    /\ (artifactState = "missing"
        \/ (artifactState = "available" /\ artifactEpoch = manifestEpoch))
    /\ buildEpoch' = graphEpoch
    /\ builder' = "staged"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState, readerEpoch, readerPath>>

MakeSegmentDurable ==
    /\ builder = "staged"
    /\ durableSegmentEpochs' = durableSegmentEpochs \cup {buildEpoch}
    /\ builder' = "durable"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, artifactEpoch, artifactState,
                    buildEpoch, readerEpoch, readerPath>>

ReplacePayload ==
    /\ builder = "durable"
    /\ NoActiveReaders
    /\ artifactState' = "mixed"
    /\ builder' = "payload_replaced"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, buildEpoch, readerEpoch, readerPath>>

ReplaceDescriptor ==
    /\ builder = "payload_replaced"
    /\ NoActiveReaders
    /\ artifactEpoch' = buildEpoch
    /\ artifactState' = "available"
    /\ builder' = "sidecar_replaced"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    buildEpoch, readerEpoch, readerPath>>

PublishManifest ==
    /\ builder = "sidecar_replaced"
    /\ buildEpoch \in durableSegmentEpochs
    /\ artifactState = "available"
    /\ artifactEpoch = buildEpoch
    /\ manifestEpoch' = buildEpoch
    /\ buildEpoch' = 0
    /\ builder' = "none"
    /\ UNCHANGED <<graphEpoch, durableSegmentEpochs, artifactEpoch,
                    artifactState, readerEpoch, readerPath>>

DiscardInvalidSidecar ==
    /\ builder = "none"
    /\ NoActiveReaders
    /\ artifactState # "missing"
    /\ (artifactState # "available" \/ artifactEpoch # manifestEpoch)
    /\ artifactState' = "missing"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, buildEpoch, builder, readerEpoch, readerPath>>

CorruptArtifact ==
    /\ builder = "none"
    /\ NoActiveReaders
    /\ artifactState = "available"
    /\ artifactState' = "corrupt"
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, buildEpoch, builder, readerEpoch, readerPath>>

BeginRead(reader) ==
    /\ readerEpoch[reader] = -1
    /\ builder \notin {"payload_replaced", "sidecar_replaced"}
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = graphEpoch]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState, buildEpoch, builder>>

UseSegment(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerPath[reader] = "none"
    /\ readerEpoch[reader] = manifestEpoch
    /\ artifactState = "available"
    /\ artifactEpoch = manifestEpoch
    /\ manifestEpoch \in durableSegmentEpochs
    /\ readerPath' = [readerPath EXCEPT ![reader] = "segment"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState, buildEpoch, builder, readerEpoch>>

FallbackToGraph(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerPath[reader] = "none"
    /\ (readerEpoch[reader] # manifestEpoch
        \/ manifestEpoch \notin durableSegmentEpochs
        \/ artifactState # "available"
        \/ artifactEpoch # manifestEpoch)
    /\ readerPath' = [readerPath EXCEPT ![reader] = "graph"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState, buildEpoch, builder, readerEpoch>>

EndRead(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = -1]
    /\ readerPath' = [readerPath EXCEPT ![reader] = "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState, buildEpoch, builder>>

Crash ==
    /\ builder' = "none"
    /\ buildEpoch' = 0
    /\ readerEpoch' = [reader \in Readers |-> -1]
    /\ readerPath' = [reader \in Readers |-> "none"]
    /\ UNCHANGED <<graphEpoch, manifestEpoch, durableSegmentEpochs,
                    artifactEpoch, artifactState>>

Next ==
    \/ CommitGraph
    \/ BeginBuild
    \/ MakeSegmentDurable
    \/ ReplacePayload
    \/ ReplaceDescriptor
    \/ PublishManifest
    \/ DiscardInvalidSidecar
    \/ CorruptArtifact
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: UseSegment(reader)
    \/ \E reader \in Readers: FallbackToGraph(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

TypeOK ==
    /\ graphEpoch \in 0..MaxEpoch
    /\ manifestEpoch \in 0..MaxEpoch
    /\ durableSegmentEpochs \subseteq 0..MaxEpoch
    /\ artifactEpoch \in 0..MaxEpoch
    /\ artifactState \in ArtifactStates
    /\ buildEpoch \in 0..MaxEpoch
    /\ builder \in BuilderStates
    /\ readerEpoch \in [Readers -> -1..MaxEpoch]
    /\ readerPath \in [Readers -> {"none", "segment", "graph"}]

ManifestEpochWasDurablyBuilt == manifestEpoch \in durableSegmentEpochs
AvailableArtifactWasDurablyBuilt ==
    artifactState = "available" => artifactEpoch \in durableSegmentEpochs
SegmentReadMatchesPinnedGraph ==
    \A reader \in Readers:
        readerPath[reader] = "segment" =>
            /\ artifactState = "available"
            /\ readerEpoch[reader] = manifestEpoch
            /\ artifactEpoch = manifestEpoch
            /\ readerEpoch[reader] \in durableSegmentEpochs
InvalidSidecarCannotBeSelected ==
    \A reader \in Readers:
        /\ readerEpoch[reader] >= 0
        /\ readerPath[reader] = "none"
        /\ (readerEpoch[reader] # manifestEpoch
            \/ manifestEpoch \notin durableSegmentEpochs
            \/ artifactState # "available"
            \/ artifactEpoch # manifestEpoch)
        => ~ENABLED UseSegment(reader)
InvalidArtifactHasNoSegmentReaders ==
    (artifactState # "available" \/ artifactEpoch # manifestEpoch) =>
        \A reader \in Readers: readerPath[reader] # "segment"

Spec == Init /\ [][Next]_vars

=============================================================================
