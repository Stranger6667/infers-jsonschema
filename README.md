# infers-jsonschema

This crate provides JSON Schema inference from input data.

Example:

```rust
use infers_jsonschema::infer;
use serde_json::json;

fn main() {
    let data = json!(["foo", "bar"]);
    let schema = infer(&data);
    assert_eq!(
        schema,
        json!({
            "type": "array",
            "items": {"type": "string"},
            "$schema": "http://json-schema.org/draft-07/schema#"
        })
    )
}
```
