use super::*;
use serde::de::{DeserializeSeed, Error, SeqAccess, Visitor};
use serde::Deserializer;
use std::fmt;

pub(super) fn visit(
    document: &SearchDocument,
    field: &str,
    visitor: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    if field != "labels" && !field.starts_with("metadata.") {
        if let Some(value) = search_document_field_value(document, field) {
            visitor(value)?;
        }
        return Ok(());
    }
    let Some(value) = document.metadata.get(field) else {
        return Ok(());
    };
    // Validate the complete array before emitting anything. Even a trailing
    // JSON error must select the legacy CSV fallback for the entire value.
    if json_strings(value, &mut |_| true).is_err() {
        for part in value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            visitor(part)?;
        }
        return Ok(());
    }
    let mut failure = None;
    let result = json_strings(value, &mut |part| {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            return true;
        }
        match visitor(trimmed) {
            Ok(()) => true,
            Err(error) => {
                failure = Some(error);
                false
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(|error| {
        SkeinError::Storage(format!("search descriptor label traversal failed: {error}"))
    })
}

fn json_strings(value: &str, visitor: &mut impl FnMut(&str) -> bool) -> serde_json::Result<()> {
    struct Strings<'a, F>(&'a mut F);
    struct StringValue<'a, F>(&'a mut F);

    impl<'de, F: FnMut(&str) -> bool> Visitor<'de> for Strings<'_, F> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array of strings")
        }

        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<(), A::Error> {
            while sequence.next_element_seed(StringValue(self.0))?.is_some() {}
            Ok(())
        }
    }

    impl<'de, F: FnMut(&str) -> bool> DeserializeSeed<'de> for StringValue<'_, F> {
        type Value = ();

        fn deserialize<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<(), D::Error> {
            deserializer.deserialize_str(self)
        }
    }

    impl<'de, F: FnMut(&str) -> bool> Visitor<'de> for StringValue<'_, F> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: Error>(self, value: &str) -> std::result::Result<(), E> {
            if (self.0)(value) {
                Ok(())
            } else {
                Err(E::custom("search descriptor label visitor stopped"))
            }
        }
    }

    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.deserialize_seq(Strings(visitor))?;
    deserializer.end()
}
