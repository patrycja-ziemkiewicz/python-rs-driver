# API Reference

## Package layout

The names most programs use come straight from `scylla`:

```python
from scylla import Consistency, ExecutionProfile, Session, SessionBuilder, Statement
```

The root also exports `Batch`, `BatchType`, `PreparedStatement`, `RequestResult`, `ScyllaError`, `SerialConsistency` and `UNSET`. Everything else lives in one of the modules below, listed in the order you meet them when writing an application.

| Module | What is in it |
|---|---|
| `scylla.session` | Connecting. `SessionBuilder` and its options (`PoolSize`, `Compression`, `WriteCoalescingDelay`, `SelfIdentity`), the `Session` it produces, and `ExecutionProfile`. |
| `scylla.statement` | What you send. `Statement`, `PreparedStatement`, `Batch` and `BatchType`, the `Consistency` and `SerialConsistency` levels, and `UNSET`, which options such as `serial_consistency` report when they are left to the execution profile. |
| `scylla.results` | What comes back. `RequestResult`, `PagingState`, the row iterators, `Column`, `ColumnSpec` and `RowFactory`. |
| `scylla.types` | The CQL type system. The `Cql*` type descriptors reported by the schema and by prepared statements, the `CqlValue` aliases describing which Python objects the driver reads and writes, and `CqlEmpty`. |
| `scylla.cluster` | What the driver knows about the cluster. `ClusterState`, `Node`, and the schema metadata: `Keyspace`, `Table`, `MaterializedView`, `Column`, `Strategy`. |
| `scylla.policies` | Tuning how requests are routed and retried: load balancing, retry, speculative execution, host filters, address translation and timestamp generation. Each has its own submodule, and everything is also importable flat from `scylla.policies`. |
| `scylla.auth` | `Authenticator` and `AuthenticatorProvider` for custom authentication. Plain username and password needs nothing from here, see `SessionBuilder.user()`. |
| `scylla.tls` | `TlsContext`, `TlsConfig` and `VerifyMode`. |
| `scylla.errors` | Every exception the driver raises. They all derive from `ScyllaError`, except `FutureCancelledError`. |
| `scylla.routing` | `Token`, `ReplicaLocator`, `Shard` and `Target`, for code that cares which node a request goes to. |
| `scylla.future` | `DriverFuture`, the awaitable returned by the driver. It also offers callbacks and a blocking `result()` for code that is not async. |
