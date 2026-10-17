---------------- MODULE HawDBInternalIdPointAccess ----------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS Keys, ChooseThroughOr, ChooseThroughNot, ChooseWrongVariable,
 IgnoreAccess, IgnoreLabels, SkipResidual, SkipDedup, SkipKeyAdmission,
 SkipSourceAdmission, ReleaseEarly
VARIABLES shape, input, present, labels, residual, access, phase, candidates,
 position, output, keyGrant, sourceGrant, owned, copied
vars == <<shape,input,present,labels,residual,access,phase,candidates,
          position,output,keyGrant,sourceGrant,owned,copied>>
\* 0 abstracts NULL; it is outside the modeled internal-ID key domain.
Shapes == {"id","and","or","not","other","indexed"}
Inputs == UNION {[1..n -> Keys \union {0}]: n \in 0..3}
Requested == {input[i]: i \in 1..Len(input)} \intersect Keys
Range(s) == {s[i]: i \in 1..Len(s)}
Truth(k) == IF k \in Requested THEN "t" ELSE IF 0 \in Range(input) THEN "u" ELSE "f"
Expected(k) == CASE shape = "id" -> Truth(k) = "t"
 [] shape = "and" -> Truth(k) = "t" /\ k \in residual
 [] shape = "or" -> Truth(k) = "t" \/ k \in residual
 [] shape = "not" -> Truth(k) = "f"
 [] shape = "other" -> k \in residual
 [] OTHER -> Truth(k) = "t" /\ k \in access
Point == Range(input) \subseteq Keys /\
 (shape \in {"id","and"} \/ (ChooseThroughOr /\ shape = "or")
  \/ (ChooseThroughNot /\ shape = "not") \/ (ChooseWrongVariable /\ shape = "other")
  \/ (IgnoreAccess /\ shape = "indexed"))
RECURSIVE Ordered(_)
Ordered(set) == IF set = {} THEN <<>> ELSE
 LET first == CHOOSE k \in set: \A other \in set: k <= other
 IN <<first>> \o Ordered(set \ {first})
Plan == IF Point THEN IF SkipDedup THEN input ELSE Ordered(Requested) ELSE Ordered(Keys)
Weight(k) == IF k = 2 THEN 3 ELSE 1
Budget == 2
Init == /\ shape \in Shapes /\ input \in Inputs
 /\ present \in SUBSET Keys /\ labels \in SUBSET Keys
 /\ residual \in SUBSET Keys /\ access \in SUBSET Keys
 /\ phase = "plan" /\ candidates = <<>> /\ position = 1 /\ output = <<>>
 /\ keyGrant = 0 /\ sourceGrant = 0 /\ owned = {} /\ copied = {}
Prepare == /\ phase = "plan" /\ candidates' = Plan /\ phase' = "read"
 /\ keyGrant' = (IF Point /\ ~SkipKeyAdmission THEN Len(input) ELSE 0)
 /\ UNCHANGED <<shape,input,present,labels,residual,access,position,output,sourceGrant,owned,copied>>
Read == /\ phase = "read" /\ position <= Len(candidates)
 /\ LET k == candidates[position]
        visible == k \in present /\ (~Point \/ IgnoreLabels \/ k \in labels)
        refuse == visible /\ Weight(k) > Budget /\ ~SkipSourceAdmission
    IN /\ phase' = (IF refuse THEN "refused" ELSE IF visible THEN "copy" ELSE "read")
       /\ position' = (IF visible THEN position ELSE position+1)
       /\ sourceGrant' = (IF visible /\ ~refuse /\ ~SkipSourceAdmission THEN Weight(k) ELSE 0)
 /\ UNCHANGED <<shape,input,present,labels,residual,access,candidates,output,keyGrant,owned,copied>>
Copy == /\ phase = "copy" /\ owned' = {candidates[position]}
 /\ copied' = copied \union owned' /\ phase' = "consume"
 /\ sourceGrant' = (IF ReleaseEarly THEN 0 ELSE sourceGrant)
 /\ UNCHANGED <<shape,input,present,labels,residual,access,candidates,position,output,keyGrant>>
Consume == /\ phase = "consume"
 /\ LET k == candidates[position]
        selected == k \in labels /\
         (IF Point /\ SkipResidual THEN TRUE
          ELSE IF Point /\ IgnoreAccess /\ shape = "indexed" THEN Truth(k) = "t" ELSE Expected(k))
    IN output' = (IF selected THEN Append(output,k) ELSE output)
 /\ phase' = "release"
 /\ UNCHANGED <<shape,input,present,labels,residual,access,candidates,position,keyGrant,sourceGrant,owned,copied>>
Release == /\ phase = "release" /\ owned' = {} /\ sourceGrant' = 0
 /\ position' = position+1 /\ phase' = "read"
 /\ UNCHANGED <<shape,input,present,labels,residual,access,candidates,output,keyGrant,copied>>
Finish == /\ phase = "read" /\ position > Len(candidates) /\ phase' = "done"
 /\ UNCHANGED <<shape,input,present,labels,residual,access,candidates,position,output,keyGrant,sourceGrant,owned,copied>>
Cancel == /\ phase \in {"read","copy","consume","release"} /\ phase' = "cancelled"
 /\ owned' = {} /\ sourceGrant' = 0 /\ keyGrant' = 0 /\ candidates' = <<>>
 /\ UNCHANGED <<shape,input,present,labels,residual,access,position,output,copied>>
Next == Prepare \/ Read \/ Copy \/ Consume \/ Release \/ Finish \/ Cancel
TypeOK == /\ shape \in Shapes /\ input \in Inputs /\ position \in 1..4
 /\ phase \in {"plan","read","copy","consume","release","done","cancelled","refused"}
 /\ Range(output) \subseteq Keys /\ owned \subseteq Keys
ExactResult == phase = "done" => output = Ordered({k \in present \intersect labels: Expected(k)})
KeyOwnership == Point /\ phase \notin {"plan","cancelled"} => Len(candidates) <= keyGrant
SourceOwnership == /\ (\A k \in owned: Weight(k) <= sourceGrant)
 /\ sourceGrant <= Budget
PointScope == Point => copied \subseteq Requested \intersect present \intersect labels
NoProgressWitness == ~(phase = "done" /\ Point /\ output # <<>> /\ 2 \in present /\ 2 \notin Requested)
=============================================================================
