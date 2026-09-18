%start Query
%%
Query -> Result<Box<hawdb_cypher::Statement>, String>:
      'MATCH' '(' Ident ':' Ident ')' 'WHERE' Property '=' Parameter 'RETURN' Property AliasOpt LimitOpt
      {
          let (predicate_variable, predicate_property) = $8?;
          let (return_variable, return_property) = $12?;
          Ok(Box::new(hawdb_cypher::Statement::MatchReturn(Box::new(hawdb_cypher::MatchReturn {
              vector_seed: None,
              variable: $3?,
              label: $5?,
              properties: std::collections::BTreeMap::new(),
              expand: None,
              post_match_expand: None,
              optional_expand: None,
              optional_with: None,
              collect_with: None,
              distinct_with: None,
              with_projection: None,
              with_order_by: Vec::new(),
              with_offset: None,
              with_limit: None,
              aggregate_with: None,
              aggregate_with_filter: None,
              post_with_match: None,
              predicate: Some(hawdb_cypher::PropertyPredicate::Eq {
                  variable: predicate_variable,
                  property: predicate_property,
                  value: $10?,
              }),
              distinct: false,
              returns: vec![hawdb_cypher::ReturnItem {
                  expression: hawdb_cypher::ReturnExpression::Property {
                      variable: return_variable,
                      property: return_property,
                  },
                  alias: $13?,
              }],
              order_by: Vec::new(),
              offset: None,
              limit: $14?,
          }))))
      }
    ;

Ident -> Result<String, String>:
      'IDENT'
      {
          let lexeme = $1.map_err(|error| format!("identifier lex error: {error:?}"))?;
          Ok($lexer.span_str(lexeme.span()).to_string())
      }
    ;

Property -> Result<(String, String), String>:
      Ident '.' Ident { Ok(($1?, $3?)) }
    ;

Parameter -> Result<hawdb_cypher::ValueExpression, String>:
      'PARAM'
      {
          let lexeme = $1.map_err(|error| format!("parameter lex error: {error:?}"))?;
          let parameter = $lexer.span_str(lexeme.span());
          Ok(hawdb_cypher::ValueExpression::Parameter(parameter[1..].to_string()))
      }
    ;

AliasOpt -> Result<Option<String>, String>:
      'AS' Ident { Ok(Some($2?)) }
    | { Ok(None) }
    ;

LimitOpt -> Result<Option<hawdb_cypher::ValueExpression>, String>:
      'LIMIT' Parameter { Ok(Some($2?)) }
    | { Ok(None) }
    ;
