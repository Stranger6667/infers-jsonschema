use chrono::{DateTime, NaiveDate};
use rayon::prelude::*;
use serde_json::{json, Map, Number, Value};
use std::collections::{BTreeMap, HashSet};
use std::iter::FromIterator;

pub struct JSONSchema<'a> {
    input: &'a Value,
    detect_format: bool,
}

impl JSONSchema<'_> {
    pub fn new(input: &Value) -> JSONSchema {
        JSONSchema {
            input,
            detect_format: true,
        }
    }

    pub fn detect_format(mut self, detect_format: bool) -> Self {
        self.detect_format = detect_format;
        self
    }

    pub fn infer(&self) -> Value {
        let mut result = self._infer(self.input);
        result.as_object_mut().unwrap().insert(
            "$schema".into(),
            Value::String("http://json-schema.org/draft-07/schema#".into()),
        );
        result
    }

    fn _infer(&self, data: &Value) -> Value {
        match data {
            Value::Null => json!({"type": "null"}),
            Value::Bool(_) => json!({"type": "boolean"}),
            Value::String(string) => self.infer_string(string, self.detect_format),
            Value::Number(number) => self.infer_number(number),
            Value::Array(array) => self.infer_array(array),
            Value::Object(object) => self.infer_object(object),
        }
    }

    fn infer_string(&self, string: &str, detect_format: bool) -> Value {
        let mut data = json!({"type": "string"});
        if detect_format {
            if let Some(format_name) = infer_format(&string) {
                data["format"] = Value::String(format_name.into());
            }
        }
        data
    }

    fn infer_number(&self, number: &Number) -> Value {
        if number.is_f64() {
            json!({"type": "number"})
        } else {
            json!({"type": "integer"})
        }
    }

    /// Infer schema for an array
    fn infer_array(&self, array: &[Value]) -> Value {
        let mut data = json!({"type": "array"});
        let items: BTreeMap<String, Value> = array
            .par_iter()
            .map(|x| {
                let inferred = self._infer(x);
                (inferred.to_string(), inferred)
            })
            .collect();
        if items.len() == 1 {
            data["items"] = items.values().next().unwrap().clone();
        } else if let Some(merged) = try_merge(&items) {
            data["items"] = merged
        } else {
            let types = items.values().collect::<Vec<&Value>>();
            data["items"] = json!({ "anyOf": types });
        }
        data
    }

    /// Infer schema for JSON object
    fn infer_object(&self, object: &Map<String, Value>) -> Value {
        let mut properties = BTreeMap::new();
        let mut required = Vec::with_capacity(object.len());
        for (key, value) in object.iter() {
            required.push(key);
            properties.insert(key, self._infer(&value));
        }
        json!({"type": "object", "required": required, "properties": properties})
    }
}

/// Shortcut for inference with default settings
pub fn infer(input: &Value) -> Value {
    JSONSchema::new(input).infer()
}

/// Try to merge multiple object schemas into one
fn try_merge(data: &BTreeMap<String, Value>) -> Option<Value> {
    if data
        .values()
        .all(|item| item.get("type").unwrap() == "object")
    {
        let mut properties_types: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let mut known_required: Vec<HashSet<String>> = vec![];
        let mut new = json!({"type": "object"});
        for item in data.values() {
            let properties = item.get("properties").unwrap().as_object().unwrap();
            for (name, schema) in properties {
                let known_types = properties_types.entry(name.into()).or_insert_with(Vec::new);
                if !known_types.contains(schema) {
                    known_types.push(schema.clone())
                }
            }
            collect_required(&mut known_required, item);
        }
        let map = new.as_object_mut().unwrap();
        fill_required(map, known_required);
        fill_properties(map, properties_types);
        return Some(new);
    }
    None
}

fn collect_required(known_required: &mut Vec<HashSet<String>>, item: &Value) {
    let required = HashSet::from_iter(
        item.get("required")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().into()),
    );
    known_required.push(required);
}

/// Fill required properties
/// There will be only properties that are common to all objects
fn fill_required(map: &mut Map<String, Value>, known_required: Vec<HashSet<String>>) {
    if let Some(first_set) = known_required.first() {
        let common_required = first_set
            .iter()
            .filter(|&k| known_required.iter().all(|s| s.contains(k)))
            .map(|x| json!(x))
            .collect::<Vec<Value>>();
        if !common_required.is_empty() {
            map.insert("required".into(), Value::Array(common_required));
        }
    }
}

/// Fill "properties" with collected values.
/// Each property can be either of one type or multiple types joined via "anyOf"
fn fill_properties(map: &mut Map<String, Value>, properties_types: BTreeMap<String, Vec<Value>>) {
    let properties = map
        .entry("properties")
        .or_insert(json!({}))
        .as_object_mut()
        .unwrap();
    for (property, known_types) in properties_types {
        let types = {
            if known_types.len() == 1 {
                json!(known_types.first())
            } else {
                json!({ "anyOf": known_types })
            }
        };
        properties.insert(property, types);
    }
}

/// Infer a format of the given string.
///
/// Currently only the following formats are supported:
///   - integer
///   - date
///   - date-time
fn infer_format(string: &str) -> Option<&str> {
    if string.parse::<i32>().is_ok() {
        return Some("integer");
    } else if NaiveDate::parse_from_str(string, "%Y-%m-%d").is_ok() {
        return Some("date");
    } else if DateTime::parse_from_rfc3339(string).is_ok() {
        return Some("date-time");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_json(data: &[(Value, Value)]) {
        for (value, expected) in data {
            assert_eq!(infer(&value), *expected);
        }
    }

    #[test]
    fn test_primitive_types() {
        let cases = [
            (
                json!(null),
                json!({"type": "null", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!(1.35),
                json!({"type": "number", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!(5),
                json!({"type": "integer", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!("Test".to_owned()),
                json!({"type": "string", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
        ];
        assert_json(&cases);
    }

    #[test]
    fn test_string_format() {
        let cases = [
            (
                json!("1"),
                json!({"type": "string", "format": "integer", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!("2020-01-01"),
                json!({"type": "string", "format": "date", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!("2018-11-13T20:20:39+00:00"),
                json!({"type": "string", "format": "date-time", "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
        ];
        assert_json(&cases);
    }

    #[test]
    fn test_disabled_string_format() {
        let data = json!("2020-01-01");
        let schema = JSONSchema::new(&data).detect_format(false);
        assert_eq!(
            schema.infer(),
            json!({"type": "string", "$schema": "http://json-schema.org/draft-07/schema#"})
        );
    }

    #[test]
    fn test_disabled_string_format_nested() {
        let cases = [
            (
                json!({"key": "2020-01-01"}),
                json!({"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"], "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!(["2020-01-01"]),
                json!({"type": "array", "items": {"type": "string"}, "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
        ];
        for (value, expected) in &cases {
            let schema = JSONSchema::new(&value).detect_format(false);
            assert_eq!(schema.infer(), *expected);
        }
    }

    #[test]
    fn test_array_primitive() {
        let cases = [
            (
                json!(["test", "item"]),
                json!({"type": "array", "items": {"type": "string"}, "$schema": "http://json-schema.org/draft-07/schema#"}),
            ),
            (
                json!(["test", "item", 1]),
                json!({
                  "type": "array",
                  "items": {
                    "anyOf": [
                      {"type": "integer"},
                      {"type": "string"}
                    ]
                  },
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
        ];
        assert_json(&cases);
    }

    #[test]
    fn test_object_primitive() {
        let cases = [
            (
                json!({"key": true}),
                json!({
                  "type": "object",
                  "properties": {
                      "key": {"type": "boolean"}
                  },
                  "required": ["key"],
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
            (
                json!({"key1": true, "key2": 1}),
                json!({
                  "type": "object",
                  "properties": {
                      "key1": {"type": "boolean"},
                      "key2": {"type": "integer"}
                  },
                  "required": ["key1", "key2"],
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
        ];
        assert_json(&cases);
    }

    #[test]
    fn test_array_complex() {
        let cases = [
            (
                json!([{"a": 1}, {"a": 2}]),
                json!({
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "a": {"type": "integer"}
                    },
                    "required": ["a"]
                  },
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
            (
                json!([{"a": 1}, {"a": null}, {"a": 2}]),
                json!({
                  "type": "array",
                  "items": {
                    "type": "object",
                    "required": ["a"],
                    "properties": {
                      "a": {
                        "anyOf": [
                          {"type": "integer"},
                          {"type": "null"}
                        ]
                      }
                    }
                  },
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
            // Proper required detection.
            (
                json!([{"a": 1}, {"b": "test"}]),
                json!({
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "a": {"type": "integer"},
                      "b": {"type": "string"}
                    }
                  },
                  "$schema": "http://json-schema.org/draft-07/schema#"
                }),
            ),
        ];
        assert_json(&cases);
    }

    #[test]
    fn test_infer_via_schema() {
        let data = json!(null);
        let schema = JSONSchema::new(&data);
        assert_eq!(
            schema.infer(),
            json!({"type": "null", "$schema": "http://json-schema.org/draft-07/schema#"})
        );
    }
}