%%
(?i:match) "MATCH"
(?i:where) "WHERE"
(?i:return) "RETURN"
(?i:as) "AS"
(?i:limit) "LIMIT"
\$[A-Za-z_][A-Za-z0-9_]* "PARAM"
[A-Za-z_][A-Za-z0-9_]* "IDENT"
\( "("
\) ")"
: ":"
\. "."
= "="
[\t\n\r ]+ ;
