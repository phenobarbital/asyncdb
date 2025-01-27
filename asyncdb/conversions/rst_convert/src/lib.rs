use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};
use rayon::prelude::*;
use std::sync::Arc;
//use pyo3::exceptions::{PyTypeError, PyValueError};
// use chrono::{NaiveDate, NaiveDateTime};

// Arrow2 imports
// use arrow2::array::{
//     Array,
//     MutableArray,
//     MutableBooleanArray,
//     MutableListArray,
//     MutablePrimitiveArray,
//     MutableUtf8Array,
//     TryPush,
// };
// use arrow2::array::ListArray;
// use arrow2::chunk::Chunk;
// use arrow2::datatypes::{DataType, Field, Schema, TimeUnit};
// use arrow2::error::Error as ArrowError;
// use arrow2::io::ipc::write::{FileWriter, WriteOptions, Compression};

/// Convert `asyncpg.Record` to Python dicts
#[pyfunction]
fn todict(py: Python, records: &PyAny) -> PyResult<Vec<Py<PyDict>>> {
    let mut dicts = Vec::new();

    for record in records.iter()? {
        let record = record?;
        let dict = PyDict::new(py);

        let items = record.call_method0("items")?;
        for item_result in items.iter()? {
            let item = item_result?;
            let (key, value): (String, &PyAny) = item.extract()?;

            // Set the item in the dictionary
            dict.set_item(key, value)?;
        }

        dicts.push(dict.into());
    }

    Ok(dicts)
}

/// Python module definition
#[pymodule]
fn rst_convert(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(todict, m)?)?;
    // m.add_function(wrap_pyfunction!(toarrow, m)?)?;
    Ok(())
}
