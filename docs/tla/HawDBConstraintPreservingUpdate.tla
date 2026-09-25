---------------- MODULE HawDBConstraintPreservingUpdate ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Values, IgnoreUniqueColumns, IgnoreForeignColumns
Columns == {"primary", "unique", "declared", "foreign", "body"}
UniqueColumns == {"unique", "declared"}
Protected == {"primary"}
             \union (IF IgnoreUniqueColumns THEN {} ELSE UniqueColumns)
             \union (IF IgnoreForeignColumns THEN {} ELSE {"foreign"})
Projection(row) == <<row["primary"], row["unique"], row["declared"], row["foreign"]>>
VARIABLES before, replacement, assigned, checked
vars == <<before, replacement, assigned, checked>>
After == [c \in Columns |-> IF c \in assigned THEN replacement[c] ELSE before[c]]
Narrow == assigned \intersect Protected = {}
Init == /\ before \in [Columns -> Values]
        /\ replacement \in [Columns -> Values]
        /\ assigned \in (SUBSET Columns) \ {{}}
        /\ checked = FALSE
Next == /\ ~checked /\ checked' = TRUE
        /\ UNCHANGED <<before, replacement, assigned>>
TypeInvariant == /\ before \in [Columns -> Values]
                 /\ replacement \in [Columns -> Values]
                 /\ assigned \in (SUBSET Columns) \ {{}}
                 /\ checked \in BOOLEAN
ConstraintProjectionPreserved == checked /\ Narrow => Projection(before) = Projection(After)
NoBodyUpdateWitness == ~(checked /\ Narrow /\ before # After)
NoUnsafeUpdateWitness == ~(checked /\ ~Narrow /\ Projection(before) # Projection(After))
Spec == Init /\ [][Next]_vars
=============================================================================
