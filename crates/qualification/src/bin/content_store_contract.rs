use skein_qualification::nowledge_content_store_sql_corpus_json;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(json) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json).expect("content-store contract must serialize")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("skein-content-store-contract: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(args: impl IntoIterator<Item = String>) -> Result<serde_json::Value, String> {
    if args.into_iter().next().is_some() {
        return Err("skein-content-store-contract does not accept arguments".to_string());
    }
    nowledge_content_store_sql_corpus_json().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_schema_and_statement_corpus_identities() {
        let json = run(Vec::<String>::new()).expect("valid content-store contract");

        assert_eq!(
            json["schema"]["identity"]["protocol"],
            "skein-nowledge-content-store-schema-v1"
        );
        assert_eq!(
            json["identity"]["protocol"],
            "skein-nowledge-content-store-sql-corpus-v1"
        );
    }

    #[test]
    fn rejects_arguments() {
        assert!(run(["unexpected".to_string()]).is_err());
    }
}
