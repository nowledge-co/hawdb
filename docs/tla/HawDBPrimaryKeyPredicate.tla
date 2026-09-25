------------------ MODULE HawDBPrimaryKeyPredicate ------------------
EXTENDS Integers, FiniteSets, Sequences
CONSTANTS Keys, Values, ExtractThroughOr, DropAbsentIntent
Absent == -1
ASSUME /\ Keys # {} /\ Values # {} /\ Absent \notin Values
       /\ {ExtractThroughOr, DropAbsentIntent} \subseteq BOOLEAN
Atoms == {<<"key", k>>: k \in Keys} \union {<<"value", v>>: v \in Values}
Pairs == {<<"and", a, b>>: a,b \in Atoms}
         \union {<<"or", a, b>>: a,b \in Atoms}
         \union {<<"not", a>>: a \in Atoms}
Predicates == Atoms \union Pairs
              \union {<<"and", a, p>>: a \in Atoms, p \in Pairs}
RECURSIVE Eval(_, _, _)
Eval(p, k, v) == CASE p[1] = "key" -> k = p[2]
                   [] p[1] = "value" -> v = p[2]
                   [] p[1] = "and" -> Eval(p[2],k,v) /\ Eval(p[3],k,v)
                   [] p[1] = "or" -> Eval(p[2],k,v) \/ Eval(p[3],k,v)
                   [] OTHER -> ~Eval(p[2],k,v)
RECURSIVE Necessary(_)
Necessary(p) == CASE p[1] = "key" -> {p[2]}
                        [] p[1] = "and" -> Necessary(p[2]) \union Necessary(p[3])
                        [] p[1] = "or" /\ ExtractThroughOr -> Necessary(p[2])
                        [] OTHER -> {}
Selected(p, rows) == {k \in Keys: rows[k] # Absent /\ Eval(p,k,rows[k])}
VARIABLES predicate, before, after, checked
vars == <<predicate, before, after, checked>>
Bound == Necessary(predicate)
Narrow == Cardinality(Bound) = 1
Claimed == IF Narrow THEN
              IF DropAbsentIntent THEN {k \in Bound: before[k] # Absent} ELSE Bound
           ELSE Keys
Init == /\ checked = FALSE
        /\ predicate \in Predicates
        /\ before \in [Keys -> Values \union {Absent}]
        /\ after \in [Keys -> Values \union {Absent}]
Next == /\ ~checked /\ checked' = TRUE
        /\ UNCHANGED <<predicate, before, after>>
TypeInvariant == /\ checked \in BOOLEAN
                 /\ predicate \in Predicates
                 /\ before \in [Keys -> Values \union {Absent}]
                 /\ after \in [Keys -> Values \union {Absent}]
KeyBound == checked /\ Narrow => Selected(predicate, after) \subseteq Bound
ReplayStable == checked /\ (\A k \in Claimed: before[k] = after[k]) =>
                  Selected(predicate, before) = Selected(predicate, after)
NoResidualWitness == ~(checked /\ Narrow /\ Selected(predicate,before) = {} /\ Selected(predicate,after) # {})
NoDisjointWitness == ~(checked /\ Narrow /\ before # after /\
                       (\A k \in Bound: before[k] = after[k]) /\ Selected(predicate,before) # {})
Spec == Init /\ [][Next]_vars
=============================================================================
