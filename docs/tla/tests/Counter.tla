----------------------------- MODULE Counter -----------------------------
EXTENDS Naturals, Limit
VARIABLE value
vars == <<value>>
Init == value = 0
Next == value' = (value + 1) % Limit
Spec == Init /\ [][Next]_vars /\ WF_vars(Next)
TypeOK == value \in 0..(Limit - 1)
ZeroOnly == value = 0
EventuallyOne == <>(value = 1)
EventuallyOutside == <>(value = Limit)
=============================================================================
