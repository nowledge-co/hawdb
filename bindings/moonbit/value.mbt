///|
/// The MoonBit-native representation of a HawDB database value.
/// Null, absent columns, binary and typed vector values remain distinct.
pub(all) enum Value {
  Null
  Bool(Bool)
  Int(Int64)
  Float(Double)
  Text(String)
  Binary(Bytes)
  Uuid(String)
  Vector(Array[Double])
  List(Array[Value])
  Map(Map[String, Value])
} derive(Debug)

///|
/// Structured engine failures and transport/decoding failures.
/// The engine category remains an open string for future HawDB versions.
pub(all) suberror HawdbError {
  Engine(String, String)
  Codec(String)
  Closed
} derive(Debug)

///|
/// Stable human-readable diagnostic, independent of debug formatting.
pub fn HawdbError::describe(self : HawdbError) -> String {
  match self {
    HawdbError::Engine(kind, message) => kind + ": " + message
    HawdbError::Codec(message) => "codec: " + message
    HawdbError::Closed => "HawDB database is closed"
  }
}

///|
/// One materialized, owned result. For named column access use get().
pub(all) struct QueryResult {
  columns : Array[String]
  rows : Array[Array[Value]]
}

///|
pub fn QueryResult::get(
  self : QueryResult,
  row : Int,
  column : String,
) -> Value? {
  if row < 0 || row >= self.rows.length() {
    return None
  }
  for i = 0; i < self.columns.length(); i = i + 1 {
    if self.columns[i] == column {
      return Some(self.rows[row][i])
    }
  }
  None
}

///|
/// Encode value as native ABI JSON. Json::number with repr preserves the
/// signed Int64 lexeme without ever converting through Double.
fn value_to_json(value : Value) -> Result[Json, HawdbError] {
  value_to_json_depth(value, 0)
}

///|
fn value_to_json_depth(value : Value, depth : Int) -> Result[Json, HawdbError] {
  if depth > 64 {
    return Err(HawdbError::Codec("parameter nests deeper than 64 levels"))
  }
  match value {
    Value::Null => Ok(Json::null())
    Value::Bool(v) => Ok(Json::boolean(v))
    Value::Int(v) => Ok(Json::number(0.0, repr=v.to_string()))
    Value::Float(v) =>
      if v != v || v == @double.infinity || v == -@double.infinity {
        Err(HawdbError::Codec("non-finite float is not representable in HawDB JSON"))
      } else {
        Ok(Json::number(v))
      }
    Value::Text(v) => Ok(Json::string(v))
    Value::Binary(v) =>
      Ok(Json::object({ "$binary": Json::string(@base64.encode(v[:])) }))
    Value::Uuid(v) =>
      Ok(Json::object({ "$uuid": Json::string(v) }))
    Value::Vector(values) => {
      if values.length() == 0 {
        return Err(HawdbError::Codec("vector must contain at least one coordinate"))
      }
      let numbers : Array[Json] = []
      for value in values {
        if value != value || value == @double.infinity || value == -@double.infinity {
          return Err(HawdbError::Codec("non-finite vector element"))
        }
        numbers.push(Json::number(value))
      }
      Ok(Json::object({ "$vector": Json::array(numbers) }))
    }
    Value::List(values) => {
      let encoded : Array[Json] = []
      for entry in values {
        match value_to_json_depth(entry, depth + 1) {
          Ok(item) => encoded.push(item)
          Err(error) => return Err(error)
        }
      }
      Ok(Json::array(encoded))
    }
    Value::Map(values) => {
      let encoded : Map[String, Json] = Map([])
      for key, entry in values {
        match value_to_json_depth(entry, depth + 1) {
          Ok(item) => encoded[key] = item
          Err(error) => return Err(error)
        }
      }
      // A singleton map with a reserved tag name would otherwise be
      // confused with a native binary/UUID/vector envelope.
      if encoded.length() == 1 {
        for key, item in encoded {
          if key == "$binary" || key == "$uuid" || key == "$vector" || key == "$map" {
            return Ok(Json::object({
              "$map": Json::array([Json::array([Json::string(key), item])]),
            }))
          }
        }
      }
      Ok(Json::object(encoded))
    }
  }
}

///|
/// Decode the original JSON number lexeme. Numbers with decimal points or
/// exponents are Double; bare integer literals are exact signed Int64.
/// Any out-of-range integer is a codec error, never a rounded float.
fn json_to_value(json : Json) -> Result[Value, HawdbError] {
  json_to_value_depth(json, 0)
}

///|
fn json_to_value_depth(json : Json, depth : Int) -> Result[Value, HawdbError] {
  if depth > 64 {
    return Err(HawdbError::Codec("result nests deeper than 64 levels"))
  }
  match json {
    Json::Null => Ok(Value::Null)
    Json::True => Ok(Value::Bool(true))
    Json::False => Ok(Value::Bool(false))
    Json::String(s) => Ok(Value::Text(s))
    Json::Number(value, repr~) =>
      match repr {
        Some(token) =>
          if token.contains(".") || token.contains("e") || token.contains("E") {
            Ok(Value::Float(value))
          } else {
            {
              let n = @string.parse_int64(token[:]) catch {
                _ => return Err(HawdbError::Codec("integer outside signed Int64: " + token))
              }
              Ok(Value::Int(n))
            }
          }
        None => Err(HawdbError::Codec("number lexeme was not preserved"))
      }
    Json::Array(values) => {
      let decoded : Array[Value] = []
      for item in values {
        match json_to_value_depth(item, depth + 1) {
          Ok(value) => decoded.push(value)
          Err(error) => return Err(error)
        }
      }
      Ok(Value::List(decoded))
    }
    Json::Object(values) => {
      if values.length() == 1 {
        match values.get("$map") {
          Some(Json::Array(entries)) => {
            let decoded : Map[String, Value] = Map([])
            for entry in entries {
              match entry {
                Json::Array(pair) => {
                  if pair.length() != 2 {
                    return Err(HawdbError::Codec("$map entries must contain two elements"))
                  }
                  let key = match pair[0] {
                    Json::String(key) => key
                    _ => return Err(HawdbError::Codec("$map key must be a string"))
                  }
                  match decoded.get(key) {
                    Some(_) => return Err(HawdbError::Codec("duplicate $map key: " + key))
                    None => ()
                  }
                  match json_to_value_depth(pair[1], depth + 1) {
                    Ok(value) => decoded[key] = value
                    Err(error) => return Err(error)
                  }
                }
                _ => return Err(HawdbError::Codec("$map entry must be a pair"))
              }
            }
            return Ok(Value::Map(decoded))
          }
          Some(_) => return Err(HawdbError::Codec("$map must be an array"))
          None => ()
        }
        match values.get("$binary") {
          Some(Json::String(s)) =>
            {
              let bytes = @base64.decode(s[:]) catch {
                _ => return Err(HawdbError::Codec("invalid base64 binary marker"))
              }
              return Ok(Value::Binary(bytes))
            }
          Some(_) =>
            return Err(HawdbError::Codec("$binary must be a string"))
          None => ()
        }
        match values.get("$uuid") {
          Some(Json::String(s)) => return Ok(Value::Uuid(s))
          Some(_) => return Err(HawdbError::Codec("$uuid must be a string"))
          None => ()
        }
        match values.get("$vector") {
          Some(Json::Array(items)) => {
            let coords : Array[Double] = []
            for item in items {
              match item {
                Json::Number(n, ..) =>
                  if n != n || n == @double.infinity || n == -@double.infinity {
                    return Err(HawdbError::Codec("non-finite vector coordinate"))
                  } else {
                    coords.push(n)
                  }
                _ => return Err(HawdbError::Codec("vector coordinate must be numeric"))
              }
            }
            return Ok(Value::Vector(coords))
          }
          Some(_) => return Err(HawdbError::Codec("$vector must be an array"))
          None => ()
        }
      }
      let decoded : Map[String, Value] = Map([])
      for key, item in values {
        match json_to_value_depth(item, depth + 1) {
          Ok(v) => decoded[key] = v
          Err(error) => return Err(error)
        }
      }
      Ok(Value::Map(decoded))
    }
  }
}

///|
/// Parse data returned by the engine. The preserve_number_repr flag is
/// mandatory: ordinary MoonBit JSON Number stores a Double approximation.
fn parse_hawdb_json(text : String) -> Result[Json, HawdbError] {
  {
    let json = @json.parse(text, preserve_number_repr=true, reject_duplicate_keys=true) catch {
      _ => return Err(HawdbError::Codec("malformed database response"))
    }
    Ok(json)
  }
}

///|
fn result_from_json(response : String) -> Result[QueryResult, HawdbError] {
  let json = match parse_hawdb_json(response) {
    Ok(v) => v
    Err(e) => return Err(e)
  }
  match json {
    Json::Object(fields) =>
      match (fields.get("columns"), fields.get("rows")) {
        (Some(Json::Array(column_values)), Some(Json::Array(row_values))) => {
          let columns : Array[String] = []
          for name in column_values {
            match name {
              Json::String(value) => {
                if columns.contains(value) {
                  return Err(HawdbError::Codec("duplicate result column: " + value))
                }
                columns.push(value)
              }
              _ => return Err(HawdbError::Codec("non-string column name"))
            }
          }
          let rows : Array[Array[Value]] = []
          for raw_row in row_values {
            match raw_row {
              Json::Array(cells) => {
                if cells.length() != columns.length() {
                  return Err(HawdbError::Codec("row width differs from column count"))
                }
                let row : Array[Value] = []
                for cell in cells {
                  match json_to_value(cell) {
                    Ok(v) => row.push(v)
                    Err(e) => return Err(e)
                  }
                }
                rows.push(row)
              }
              _ => return Err(HawdbError::Codec("non-array result row"))
            }
          }
          Ok({ columns, rows })
        }
        _ => Err(HawdbError::Codec("missing columns or rows in response"))
      }
    _ => Err(HawdbError::Codec("result must be an object"))
  }
}

///|
fn native_error(response : String) -> HawdbError {
  let parsed = @json.parse(response) catch {
    _ => return HawdbError::Codec("malformed native error envelope: " + response)
  }
  match parsed {
    Json::Object(fields) =>
      match (fields.get("kind"), fields.get("message")) {
        (Some(Json::String("closed")), _) => HawdbError::Closed
        (Some(Json::String(kind)), Some(Json::String(message))) =>
          HawdbError::Engine(kind, message)
        _ => HawdbError::Codec("malformed native error envelope: " + response)
      }
    _ => HawdbError::Codec("malformed native error envelope: " + response)
  }
}

///|
/// Internal, shared parameter codecs for Database and Transaction. Keeping
/// both call paths here prevents differences in nesting and number handling.
fn encode_named_params(params : Map[String, Value]) -> Result[String, HawdbError] {
  let encoded : Map[String, Json] = Map([])
  for name, item in params {
    match value_to_json(item) {
      Ok(value) => encoded[name] = value
      Err(error) => return Err(error)
    }
  }
  Ok(Json::object(encoded).stringify())
}

///|
fn encode_sql_params(params : Array[Value]) -> Result[String, HawdbError] {
  let encoded : Array[Json] = []
  for item in params {
    match value_to_json(item) {
      Ok(value) => encoded.push(value)
      Err(error) => return Err(error)
    }
  }
  Ok(Json::array(encoded).stringify())
}

///|
/// Cypher with typed, named parameters, returning owned typed rows.
pub fn Database::query(
  self : Database,
  statement : String,
  params : Map[String, Value],
) -> Result[QueryResult, HawdbError] {
  if !self.is_open() {
    return Err(HawdbError::Closed)
  }
  let json = match encode_named_params(params) {
    Ok(json) => json
    Err(error) => return Err(error)
  }
  match self.query_raw(statement, json) {
    Ok(text) => result_from_json(text)
    Err(error) => Err(native_error(error))
  }
}

///|
/// SQL with typed, positional parameters, returning owned typed rows.
pub fn Database::query_sql(
  self : Database,
  statement : String,
  params : Array[Value],
) -> Result[QueryResult, HawdbError] {
  if !self.is_open() {
    return Err(HawdbError::Closed)
  }
  let json = match encode_sql_params(params) {
    Ok(json) => json
    Err(error) => return Err(error)
  }
  match self.query_sql_raw(statement, json) {
    Ok(text) => result_from_json(text)
    Err(error) => Err(native_error(error))
  }
}

///|
/// Native MoonBit error propagation for callers preferring try/catch.
pub fn Database::query_or_raise(
  self : Database,
  statement : String,
  params : Map[String, Value],
) -> QueryResult raise HawdbError {
  match self.query(statement, params) {
    Ok(value) => value
    Err(error) => raise error
  }
}

///|
pub fn Database::query_sql_or_raise(
  self : Database,
  statement : String,
  params : Array[Value],
) -> QueryResult raise HawdbError {
  match self.query_sql(statement, params) {
    Ok(value) => value
    Err(error) => raise error
  }
}
