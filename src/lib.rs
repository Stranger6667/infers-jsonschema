use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyAny, PyDict, PyFloat, PyList, PyTuple};
use pyo3::wrap_pyfunction;
use pyo3::Python;
use serde::ser::{self, Serialize, SerializeMap, SerializeSeq};
use serde::Serializer;
use serde_json::{Error, Value};

pub mod inference;

struct ValueWrapper<'a> {
    obj: &'a PyAny,
}

/// Convert a Python value to serde_json::Value
impl<'a> Serialize for ValueWrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        macro_rules! cast {
            ($f:expr) => {
                if let Ok(val) = PyTryFrom::try_from(self.obj) {
                    return $f(val);
                }
            };
        }
        macro_rules! extract {
            ($t:ty) => {
                if let Ok(val) = <$t as FromPyObject>::extract(self.obj) {
                    return val.serialize(serializer);
                }
            };
        }

        cast!(|x: &PyList| {
            let mut seq = serializer.serialize_seq(Some(x.len()))?;
            for element in x {
                seq.serialize_element(&ValueWrapper { obj: element })?
            }
            seq.end()
        });
        cast!(|x: &PyTuple| {
            let mut seq = serializer.serialize_seq(Some(x.len()))?;
            for element in x {
                seq.serialize_element(&ValueWrapper { obj: element })?
            }
            seq.end()
        });
        cast!(|x: &PyDict| {
            let mut map = serializer.serialize_map(Some(x.len()))?;
            for (key, value) in x {
                if key.is_none() {
                    map.serialize_key("null")?;
                } else if let Ok(key) = key.extract::<bool>() {
                    map.serialize_key(if key { "true" } else { "false" })?;
                } else if let Ok(key) = key.str() {
                    let key = key.to_string().unwrap();
                    map.serialize_key(&key)?;
                } else {
                    return Err(ser::Error::custom(format_args!(
                        "Dictionary key is not a string: {:?}",
                        key
                    )));
                }
                map.serialize_value(&ValueWrapper { obj: value })?;
            }
            map.end()
        });
        extract!(String);
        extract!(bool);
        extract!(i64);
        cast!(|x: &PyFloat| {
            let v = x.value();
            if !v.is_normal() {
                return Err(ser::Error::custom(format!("Can't represent {} as JSON", v)));
            }
            v.serialize(serializer)
        });
        if self.obj.is_none() {
            return serializer.serialize_unit();
        }
        match self.obj.repr() {
            Ok(repr) => Err(ser::Error::custom(format_args!(
                "Can't convert to JSON: {}",
                repr,
            ))),
            Err(_) => Err(ser::Error::custom(format_args!(
                "Type is not JSON serializable: {}",
                self.obj.get_type().name().into_owned(),
            ))),
        }
    }
}

enum InferenceError {
    SerdeError(Error),
}

impl std::convert::From<InferenceError> for PyErr {
    fn from(error: InferenceError) -> Self {
        match error {
            InferenceError::SerdeError(err) => exceptions::ValueError::py_err(err.to_string()),
        }
    }
}

#[pyclass]
struct Schema {
    data: Value,
}

#[pymethods]
impl Schema {
    fn to_string(&self) -> PyResult<String> {
        Ok(self.data.to_string())
    }

    fn to_py(&self, py: Python) -> PyResult<PyObject> {
        into_py(&self.data, py)
    }
}

fn into_py(value: &Value, py: Python) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(value) => Ok(PyObject::from_py(*value, py)),
        Value::String(value) => Ok(value.into_py(py)),
        Value::Number(value) => {
            if value.is_f64() {
                Ok(value.as_f64().into_py(py))
            } else {
                Ok(value.as_i64().into_py(py))
            }
        }
        Value::Object(map) => {
            let iter = map.into_iter().map(|(k, v)| (k, into_py(v, py).unwrap()));
            Ok(IntoPyDict::into_py_dict(iter, py).into_py(py))
        }
        Value::Array(items) => Ok(PyList::new(
            py,
            items
                .iter()
                .map(|item| into_py(item, py).unwrap())
                .collect::<Vec<_>>(),
        )
        .to_object(py)),
    }
}

fn from_value(data: &Value, detect_format: Option<bool>) -> Schema {
    let mut schema = inference::JSONSchema::new(&data);
    if let Some(value) = detect_format {
        schema = schema.detect_format(value);
    }
    Schema {
        data: schema.infer(),
    }
}

#[pyfunction]
#[text_signature = "(string, detect_format = True)"]
fn from_py(item: &'static PyAny, detect_format: Option<bool>) -> PyResult<Schema> {
    let value =
        serde_json::to_value(ValueWrapper { obj: item }).map_err(InferenceError::SerdeError)?;
    Ok(from_value(&value, detect_format))
}

#[pyfunction]
#[text_signature = "(string, detect_format = True)"]
fn from_string(string: &str, detect_format: Option<bool>) -> PyResult<Schema> {
    let value = serde_json::from_str(string).map_err(InferenceError::SerdeError)?;
    Ok(from_value(&value, detect_format))
}

#[pymodule]
fn infers_jsonschema(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(from_string))?;
    m.add_wrapped(wrap_pyfunction!(from_py))?;
    Ok(())
}
