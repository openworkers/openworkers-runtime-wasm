//! Host side of the `openworkers:bindings` package
//!
//! Every call is forwarded to the runner's `OperationsHandler` under the
//! binding name the guest passed. Per-binding quotas live in the runner's
//! handler, which already holds the `BindingLimit` semaphores.

use crate::worker::WasmState;
use openworkers_core::{
    DatabaseOp, DatabaseResult, KvOp, KvResult, OperationsHandle, SqlParam, SqlPrimitive,
    StorageOp, StorageResult,
};
use std::time::Instant;
use wasmtime::component::bindgen;

bindgen!({
    path: "wit",
    world: "worker-host",
    imports: { default: async },
    exports: { default: async },
});

/// The task export, in a module of its own because a second world's
/// generated names would collide with the first's.
pub mod task {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "task-host",
        exports: { default: async },
    });
}

use openworkers::bindings::database;
use openworkers::bindings::kv;
use openworkers::bindings::storage;

impl database::Host for WasmState {
    async fn first(
        &mut self,
        binding: String,
        sql: String,
        params: Vec<database::SqlParam>,
    ) -> Result<Option<String>, String> {
        let (rows, _) = query(self.ops()?, binding, sql, params).await?;

        let Some(first) = rows.as_array().and_then(|rows| rows.first()) else {
            return Ok(None);
        };

        Ok(Some(first.to_string()))
    }

    async fn all(
        &mut self,
        binding: String,
        sql: String,
        params: Vec<database::SqlParam>,
    ) -> Result<database::QueryResult, String> {
        let (rows, meta) = query(self.ops()?, binding, sql, params).await?;

        Ok(database::QueryResult {
            rows: match rows.is_array() {
                true => rows.to_string(),
                false => "[]".to_string(),
            },
            meta,
        })
    }

    async fn run(
        &mut self,
        binding: String,
        sql: String,
        params: Vec<database::SqlParam>,
    ) -> Result<database::QueryMeta, String> {
        let (_, meta) = query(self.ops()?, binding, sql, params).await?;

        Ok(meta)
    }
}

/// Run one statement and split the handler's JSON into rows and meta
async fn query(
    ops: OperationsHandle,
    binding: String,
    sql: String,
    params: Vec<database::SqlParam>,
) -> Result<(serde_json::Value, database::QueryMeta), String> {
    let op = DatabaseOp::Query {
        sql,
        params: params.into_iter().map(sql_param).collect(),
    };

    let started = Instant::now();

    let rows: serde_json::Value = match ops.handle_binding_database(&binding, op).await {
        DatabaseResult::Rows(json) => serde_json::from_str(&json)
            .map_err(|e| format!("database returned invalid JSON: {}", e))?,
        DatabaseResult::Table { columns, rows } => table_rows(&columns, rows)?,
        DatabaseResult::Error(e) => return Err(e),
    };

    // Row-returning statements come back as an array; a mutation without
    // RETURNING reports its count instead
    let rows_affected = match &rows {
        serde_json::Value::Array(rows) => rows.len() as u64,
        rows => rows
            .get("rowsAffected")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    };

    let meta = database::QueryMeta {
        rows_affected,
        duration_ms: started.elapsed().as_millis() as u64,
    };

    Ok((rows, meta))
}

/// Render typed rows as the JSON array the WIT carries, `SqlPrimitive::Bytes`
/// tagged the way core serializes it. Bindings 0.2.0 carries `list<sql-value>`
/// and needs none of this.
fn table_rows(
    columns: &[String],
    rows: Vec<Vec<SqlPrimitive>>,
) -> Result<serde_json::Value, String> {
    let mut out = Vec::with_capacity(rows.len());

    for (index, row) in rows.into_iter().enumerate() {
        if row.len() != columns.len() {
            return Err(format!(
                "database returned {} values for {} columns in row {}",
                row.len(),
                columns.len(),
                index
            ));
        }

        let mut object = serde_json::Map::with_capacity(columns.len());

        for (column, value) in columns.iter().zip(row) {
            let value = serde_json::to_value(value)
                .map_err(|e| format!("database returned an unrepresentable value: {}", e))?;

            object.insert(column.clone(), value);
        }

        out.push(serde_json::Value::Object(object));
    }

    Ok(serde_json::Value::Array(out))
}

fn sql_param(param: database::SqlParam) -> SqlParam {
    match param {
        database::SqlParam::Value(value) => SqlParam::Primitive(sql_primitive(value)),
        database::SqlParam::Values(values) => {
            SqlParam::Array(values.into_iter().map(sql_primitive).collect())
        }
    }
}

fn sql_primitive(value: database::SqlValue) -> SqlPrimitive {
    match value {
        database::SqlValue::Null => SqlPrimitive::Null,
        database::SqlValue::Boolean(b) => SqlPrimitive::Bool(b),
        database::SqlValue::Integer(i) => SqlPrimitive::Int(i),
        database::SqlValue::Float(f) => SqlPrimitive::Float(f),
        database::SqlValue::Text(s) => SqlPrimitive::String(s),
    }
}

impl kv::Host for WasmState {
    async fn get(&mut self, binding: String, key: String) -> Result<Option<String>, String> {
        match self
            .ops()?
            .handle_binding_kv(&binding, KvOp::Get { key })
            .await
        {
            KvResult::Value(value) => Ok(value.map(|value| value.to_string())),
            KvResult::Error(e) => Err(e),
            _ => Err("kv get returned an unexpected result".to_string()),
        }
    }

    async fn put(
        &mut self,
        binding: String,
        key: String,
        value: String,
        expires_in_seconds: Option<u64>,
    ) -> Result<(), String> {
        let value = serde_json::from_str(&value)
            .map_err(|e| format!("kv put value is not a JSON document: {}", e))?;

        let op = KvOp::Put {
            key,
            value,
            expires_in: expires_in_seconds,
        };

        match self.ops()?.handle_binding_kv(&binding, op).await {
            KvResult::Error(e) => Err(e),
            _ => Ok(()),
        }
    }

    async fn delete(&mut self, binding: String, key: String) -> Result<(), String> {
        match self
            .ops()?
            .handle_binding_kv(&binding, KvOp::Delete { key })
            .await
        {
            KvResult::Error(e) => Err(e),
            _ => Ok(()),
        }
    }

    async fn list_keys(
        &mut self,
        binding: String,
        prefix: Option<String>,
        limit: Option<u32>,
    ) -> Result<Vec<String>, String> {
        let op = KvOp::List { prefix, limit };

        match self.ops()?.handle_binding_kv(&binding, op).await {
            KvResult::Keys(keys) => Ok(keys),
            KvResult::Error(e) => Err(e),
            _ => Err("kv list returned an unexpected result".to_string()),
        }
    }
}

impl storage::Host for WasmState {
    async fn get(&mut self, binding: String, key: String) -> Result<Option<Vec<u8>>, String> {
        let op = StorageOp::Get { key };

        match self.ops()?.handle_binding_storage(&binding, op).await {
            StorageResult::Body(body) => Ok(body),
            StorageResult::Error(e) => Err(e),
            _ => Err("storage get returned an unexpected result".to_string()),
        }
    }

    async fn put(&mut self, binding: String, key: String, body: Vec<u8>) -> Result<(), String> {
        let op = StorageOp::Put { key, body };

        match self.ops()?.handle_binding_storage(&binding, op).await {
            StorageResult::Error(e) => Err(e),
            _ => Ok(()),
        }
    }

    async fn delete(&mut self, binding: String, key: String) -> Result<(), String> {
        let op = StorageOp::Delete { key };

        match self.ops()?.handle_binding_storage(&binding, op).await {
            StorageResult::Error(e) => Err(e),
            _ => Ok(()),
        }
    }

    async fn head(&mut self, binding: String, key: String) -> Result<storage::ObjectInfo, String> {
        let op = StorageOp::Head { key };

        match self.ops()?.handle_binding_storage(&binding, op).await {
            StorageResult::Head { size, etag } => Ok(storage::ObjectInfo { size, etag }),
            StorageResult::Error(e) => Err(e),
            _ => Err("storage head returned an unexpected result".to_string()),
        }
    }

    async fn list_keys(
        &mut self,
        binding: String,
        prefix: Option<String>,
        limit: Option<u32>,
    ) -> Result<storage::Listing, String> {
        let op = StorageOp::List { prefix, limit };

        match self.ops()?.handle_binding_storage(&binding, op).await {
            StorageResult::List { keys, truncated } => Ok(storage::Listing { keys, truncated }),
            StorageResult::Error(e) => Err(e),
            _ => Err("storage list returned an unexpected result".to_string()),
        }
    }
}
