------------------- MODULE HawDBValidatedResultDelivery -------------------
EXTENDS Naturals, Sequences

(* Public request consumers always defer callbacks until every output limit
   and result-memory admission has succeeded. Callback failure/cancellation
   after validation may leave an observable prefix, never a budget failure. *)
CONSTANT Payloads, Memory, MaxRows, MaxPayload, MaxMemory, MutateEarlyDelivery
ASSUME /\ Len(Payloads) = Len(Memory)
       /\ Len(Payloads) > 0
       /\ Payloads \in Seq(Nat \ {0})
       /\ Memory \in Seq(Nat \ {0})
       /\ MutateEarlyDelivery \in BOOLEAN

ExamplePayloads == <<1, 2, 1>>
ExampleMemory == <<2, 3, 2>>

Phases == {"collecting", "delivering", "succeeded", "failed", "cancelled"}
VARIABLES rowLimit, payloadLimit, memoryLimit, produced, bytes, reserved,
          emitted, validated, phase
vars == <<rowLimit, payloadLimit, memoryLimit, produced, bytes, reserved,
          emitted, validated, phase>>

Init ==
    /\ rowLimit \in 0..MaxRows
    /\ payloadLimit \in 0..MaxPayload
    /\ memoryLimit \in 0..MaxMemory
    /\ produced = 0
    /\ bytes = 0
    /\ reserved = 0
    /\ emitted = <<>>
    /\ validated = FALSE
    /\ phase = "collecting"

FitsNext ==
    /\ produced < rowLimit
    /\ bytes + Payloads[produced + 1] <= payloadLimit
    /\ reserved + Memory[produced + 1] <= memoryLimit

Collect ==
    /\ phase = "collecting"
    /\ produced < Len(Payloads)
    /\ FitsNext
    /\ produced' = produced + 1
    /\ bytes' = bytes + Payloads[produced + 1]
    /\ reserved' = reserved + Memory[produced + 1]
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, emitted, validated, phase>>

RejectNext ==
    /\ phase = "collecting"
    /\ produced < Len(Payloads)
    /\ ~FitsNext
    /\ phase' = "failed"
    /\ reserved' = 0
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, emitted, validated>>

Validate ==
    /\ phase = "collecting"
    /\ produced = Len(Payloads)
    /\ validated' = TRUE
    /\ phase' = "delivering"
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, reserved, emitted>>

Deliver ==
    /\ phase = "delivering" \/ (MutateEarlyDelivery /\ phase = "collecting")
    /\ Len(emitted) < produced
    /\ emitted' = Append(emitted, Len(emitted) + 1)
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, reserved, validated, phase>>

ConsumerFails ==
    /\ phase = "delivering"
    /\ Len(emitted) < produced
    (* Callback side effects are observable even when it returns Err. *)
    /\ emitted' = Append(emitted, Len(emitted) + 1)
    /\ phase' = "failed"
    /\ reserved' = 0
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, validated>>

Stop(reason) ==
    /\ phase \in {"collecting", "delivering"}
    /\ phase' = reason
    /\ reserved' = 0
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, emitted, validated>>

Complete ==
    /\ phase = "delivering"
    /\ Len(emitted) = produced
    /\ phase' = "succeeded"
    /\ reserved' = 0
    /\ UNCHANGED <<rowLimit, payloadLimit, memoryLimit, produced, bytes, emitted, validated>>

Next == Collect \/ RejectNext \/ Validate \/ Deliver \/ ConsumerFails
        \/ Stop("failed") \/ Stop("cancelled") \/ Complete

TypeOK ==
    /\ rowLimit \in 0..MaxRows
    /\ payloadLimit \in 0..MaxPayload
    /\ memoryLimit \in 0..MaxMemory
    /\ produced \in 0..Len(Payloads)
    /\ bytes \in 0..payloadLimit
    /\ reserved \in 0..memoryLimit
    /\ emitted \in Seq(1..Len(Payloads))
    /\ validated \in BOOLEAN
    /\ phase \in Phases

NoUnvalidatedDelivery == Len(emitted) > 0 => validated
OrderedPrefix ==
    /\ Len(emitted) <= produced
    /\ \A index \in 1..Len(emitted): emitted[index] = index
ValidationCoversWholeInput == validated => produced = Len(Payloads)
TerminalReleasesResult == phase \in {"succeeded", "failed", "cancelled"} => reserved = 0
SuccessIsComplete == phase = "succeeded" => /\ validated /\ Len(emitted) = Len(Payloads)

Spec == Init /\ [][Next]_vars
=============================================================================
