//! Errors of the legacy (`cassandra-driver` compatible) API.

use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(scylla.legacy, QueryExhausted, PyException);
