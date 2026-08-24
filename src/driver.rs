use irodori_connector_abi::{option_string, percent_encode, push_sensitive};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, BigQueryConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct BigQueryConnection {
    client: Client,
    config: BigQueryConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BigQueryConfig {
    project_id: String,
    access_token: String,
    location: Option<String>,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    columns: Vec<Value>,
}

#[derive(Deserialize)]
struct GcpServiceAccountKey {
    project_id: String,
    client_email: String,
    private_key: String,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, BigQueryConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match runtime()
        .and_then(|runtime| runtime.block_on(BigQueryConfig::from_request(request)))
    {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let connection = BigQueryConnection {
        client: Client::new(),
        config,
    };
    let dataset_count = match runtime().and_then(|runtime| runtime.block_on(probe(&connection))) {
        Ok(count) => count,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let mut response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "projectId".to_string(),
            Value::String(connection.config.project_id.clone()),
        ),
        ("datasetCount".to_string(), json!(dataset_count)),
        (
            "serverVersion".to_string(),
            Value::String("Google BigQuery v2 API".to_string()),
        ),
    ]);
    if let Some(location) = connection.config.location.as_deref() {
        response.insert("location".to_string(), Value::String(location.to_string()));
    }
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime()
        .and_then(|runtime| runtime.block_on(run_query(&connection, sql, abi::max_rows(request))))
    {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

impl BigQueryConfig {
    async fn from_request(request: &Value) -> Result<Self, String> {
        let service_json = option_string(
            request,
            &["serviceAccountJson", "credentialsJson", "serviceAccountKey"],
        )
        .or_else(|| {
            option_string(request, &["password", "privateKey"])
                .filter(|value| value.trim_start().starts_with('{'))
        });
        let (project_id, access_token) = if let Some(service_json) = service_json {
            let key: GcpServiceAccountKey = serde_json::from_str(&service_json)
                .map_err(|err| format!("invalid Google service account JSON: {err}"))?;
            let token =
                fetch_oauth2_token(&Client::new(), &key.client_email, &key.private_key).await?;
            (key.project_id, token)
        } else {
            let project_id =
                option_string(request, &["projectId", "project", "database", "db", "host"])
                    .ok_or_else(|| "BigQuery requires projectId, database, or host.".to_string())?;
            let access_token = option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok());

            // Nothing supplied: fall back to Application Default Credentials

            // rather than refusing. On a developer machine that means the

            // `gcloud` login already there, and on GCE/GKE/Cloud Run the

            // metadata server, which is why ADC works with nothing configured.

            let access_token = match access_token {
                Some(token) => token,

                None => {
                    fetch_adc_token(&Client::new(), "https://www.googleapis.com/auth/bigquery")
                        .await?
                }
            };
            (project_id, access_token)
        };
        // Borrow another service account's permissions without holding its key.
        let access_token = match option_string(
            request,
            &["impersonateServiceAccount", "serviceAccountImpersonation"],
        ) {
            Some(target) => {
                let delegates: Vec<String> = option_string(request, &["impersonationDelegates"])
                    .map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|part| !part.is_empty())
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                impersonate_service_account(
                    &Client::new(),
                    &access_token,
                    &target,
                    "https://www.googleapis.com/auth/bigquery",
                    &delegates,
                )
                .await?
            }
            None => access_token,
        };
        let location = option_string(request, &["location", "region"]);
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&access_token));
        Ok(Self {
            project_id,
            access_token,
            location,
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        abi::redact(message, &self.redaction_values)
    }
}

async fn probe(connection: &BigQueryConnection) -> Result<usize, String> {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets?maxResults=1",
        connection.config.project_id
    );
    let value = request_json(connection, connection.client.get(url)).await?;
    Ok(value
        .get("datasets")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0))
}

async fn run_query(
    connection: &BigQueryConnection,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
        connection.config.project_id
    );
    let mut payload = json!({
        "query": sql,
        "useLegacySql": false,
        "maxResults": cap.min(10_000),
        "timeoutMs": 30_000
    });
    if let Some(location) = connection.config.location.as_deref() {
        payload["location"] = Value::String(location.to_string());
    }
    let mut value = request_json(connection, connection.client.post(url).json(&payload)).await?;
    if let Some(error) = query_error(&value) {
        return Err(error);
    }
    let job_reference = value.get("jobReference").cloned();
    for _ in 0..120 {
        if value
            .get("jobComplete")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        value = get_query_results(connection, job_reference.as_ref(), None, cap).await?;
        if let Some(error) = query_error(&value) {
            return Err(error);
        }
    }
    let (columns, mut rows, mut truncated, mut page_token) = parse_query_response(&value, cap);
    while rows.len() < cap {
        let Some(token) = page_token.take() else {
            break;
        };
        let next = get_query_results(connection, job_reference.as_ref(), Some(&token), cap).await?;
        if let Some(error) = query_error(&next) {
            return Err(error);
        }
        let (_, next_rows, next_truncated, next_page) =
            parse_query_response(&next, cap - rows.len());
        rows.extend(next_rows);
        truncated |= next_truncated;
        page_token = next_page;
    }
    if page_token.is_some() {
        truncated = true;
    }
    Ok((columns, rows, truncated))
}

async fn get_query_results(
    connection: &BigQueryConnection,
    job_reference: Option<&Value>,
    page_token: Option<&str>,
    cap: usize,
) -> Result<Value, String> {
    let job_id = job_reference
        .and_then(|value| value.get("jobId"))
        .and_then(Value::as_str)
        .ok_or_else(|| "BigQuery response missing jobReference.jobId.".to_string())?;
    let location = job_reference
        .and_then(|value| value.get("location"))
        .and_then(Value::as_str)
        .or(connection.config.location.as_deref());
    let mut url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries/{job_id}?maxResults={}",
        connection.config.project_id,
        cap.min(10_000)
    );
    if let Some(location) = location {
        url.push_str("&location=");
        url.push_str(location);
    }
    if let Some(page_token) = page_token {
        url.push_str("&pageToken=");
        url.push_str(page_token);
    }
    request_json(connection, connection.client.get(url)).await
}

fn parse_query_response(
    value: &Value,
    cap: usize,
) -> (Vec<String>, Vec<Vec<Value>>, bool, Option<String>) {
    let columns = value
        .pointer("/schema/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| field.get("name").and_then(Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut rows = Vec::new();
    if let Some(rowset) = value.get("rows").and_then(Value::as_array) {
        for row in rowset {
            if rows.len() >= cap {
                break;
            }
            let values = row
                .get("f")
                .and_then(Value::as_array)
                .map(|cells| {
                    cells
                        .iter()
                        .map(|cell| cell.get("v").cloned().unwrap_or(Value::Null))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec![Value::Null; columns.len()]);
            rows.push(values);
        }
    }
    let page_token = value
        .get("pageToken")
        .and_then(Value::as_str)
        .map(str::to_string);
    let truncated = page_token.is_some()
        || value
            .get("totalRows")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<usize>().ok())
            .map(|total| total > rows.len())
            .unwrap_or(false);
    (columns, rows, truncated, page_token)
}

fn query_error(value: &Value) -> Option<String> {
    value
        .get("errors")
        .and_then(Value::as_array)
        .and_then(|errors| errors.first())
        .and_then(|error| error.get("message").and_then(Value::as_str))
        .map(str::to_string)
}

async fn load_metadata(connection: &BigQueryConnection) -> Result<Value, String> {
    let datasets_url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
        connection.config.project_id
    );
    let value = request_json(connection, connection.client.get(datasets_url)).await?;
    let datasets = value
        .get("datasets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|dataset| {
            dataset
                .pointer("/datasetReference/datasetId")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let mut schemas: BTreeMap<String, BTreeMap<String, ObjectMeta>> = BTreeMap::new();
    for dataset in datasets {
        schemas.entry(dataset.clone()).or_default();
        let sql = format!(
            "SELECT table_name, column_name, data_type, ordinal_position, is_nullable \
             FROM `{}`.INFORMATION_SCHEMA.COLUMNS \
             ORDER BY table_name, ordinal_position",
            dataset.replace('`', "")
        );
        let Ok((columns, rows, _)) = run_query(connection, &sql, 10_000).await else {
            continue;
        };
        for row in rows {
            let table = field(&columns, &row, "table_name").unwrap_or_default();
            let column = field(&columns, &row, "column_name").unwrap_or_default();
            if table.is_empty() || column.is_empty() {
                continue;
            }
            let object = schemas
                .entry(dataset.clone())
                .or_default()
                .entry(table)
                .or_default();
            object.columns.push(json!({
                "name": column,
                "dataType": field(&columns, &row, "data_type").unwrap_or_default(),
                "nullable": field(&columns, &row, "is_nullable")
                    .map(|value| value.eq_ignore_ascii_case("YES") || value.eq_ignore_ascii_case("true"))
                    .unwrap_or(true),
                "ordinal": field(&columns, &row, "ordinal_position")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or((object.columns.len() + 1) as i64)
            }));
        }
    }
    Ok(json!({
        "schemas": schemas
            .into_iter()
            .map(|(schema, objects)| json!({
                "name": schema,
                "objects": objects
                    .into_iter()
                    .map(|(name, object)| json!({
                        "schema": schema,
                        "name": name,
                        "kind": "table",
                        "columns": object.columns,
                        "indexes": [],
                        "primaryKey": [],
                        "foreignKeys": []
                    }))
                    .collect::<Vec<_>>()
            }))
            .collect::<Vec<_>>()
    }))
}

async fn request_json(
    connection: &BigQueryConnection,
    builder: reqwest::RequestBuilder,
) -> Result<Value, String> {
    let response = builder
        .bearer_auth(&connection.config.access_token)
        .send()
        .await
        .map_err(|err| format!("BigQuery request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("BigQuery response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("BigQuery returned HTTP {status}: {text}"));
    }
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("BigQuery JSON response parse failed: {err}: {text}"))
}

/// A credential file as Application Default Credentials stores it.
///
/// ADC is not one thing. `gcloud auth application-default login` writes an
/// `authorized_user` file — a refresh token, not a key — while a service
/// account downloaded from the console writes a `service_account` file, and
/// workload identity federation writes `external_account`. They need three
/// different exchanges, and reading the `type` field is the only way to know
/// which one is in front of you.
#[derive(Debug, PartialEq, Eq)]
enum AdcKind {
    ServiceAccount,
    AuthorizedUser,
    ExternalAccount,
    Unknown(String),
}

fn adc_kind(document: &Value) -> AdcKind {
    match document.get("type").and_then(Value::as_str) {
        Some("service_account") => AdcKind::ServiceAccount,
        Some("authorized_user") => AdcKind::AuthorizedUser,
        Some("external_account") => AdcKind::ExternalAccount,
        Some(other) => AdcKind::Unknown(other.to_string()),
        None => AdcKind::Unknown(String::new()),
    }
}

/// Where Application Default Credentials looks for a credential file.
///
/// `GOOGLE_APPLICATION_CREDENTIALS` first, then the well-known path
/// `gcloud auth application-default login` writes to — the same order the
/// Google client libraries use, so a machine already set up for `gcloud` needs
/// no configuration here at all.
fn adc_paths() -> Vec<String> {
    adc_paths_from(
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .ok()
            .as_deref(),
        std::env::var("CLOUDSDK_CONFIG").ok().as_deref(),
        std::env::var("HOME").ok().as_deref(),
    )
}

/// The search order itself, with the environment passed in.
///
/// Kept pure so it can be tested without `set_var`: the environment is
/// process-global, so env-mutating tests race each other under the default
/// parallel runner and fail in a way that looks like a logic bug.
fn adc_paths_from(
    explicit: Option<&str>,
    cloudsdk_config: Option<&str>,
    home: Option<&str>,
) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(explicit) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        paths.push(explicit.to_string());
    }
    let config_dir = cloudsdk_config
        .map(str::to_string)
        .or_else(|| home.map(|home| format!("{home}/.config/gcloud")));
    if let Some(config_dir) = config_dir {
        paths.push(format!("{config_dir}/application_default_credentials.json"));
    }
    paths
}

/// Exchange an `authorized_user` refresh token for an access token.
async fn fetch_refresh_token_grant(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, String> {
    let body = format!(
        "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
        percent_encode(client_id),
        percent_encode(client_secret),
        percent_encode(refresh_token)
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("Google token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Google token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "Google returned HTTP {status} for the token request."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "Google token response contained no access_token.".to_string())
}

/// Resolve an access token from Application Default Credentials.
async fn fetch_adc_token(client: &Client, scope: &str) -> Result<String, String> {
    let mut tried = Vec::new();
    for path in adc_paths() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            tried.push(path);
            continue;
        };
        let document: Value = serde_json::from_str(&text)
            .map_err(|err| format!("credential file at {path} is not valid JSON: {err}"))?;
        return match adc_kind(&document) {
            AdcKind::ServiceAccount => {
                let key: GcpServiceAccountKey =
                    serde_json::from_value(document).map_err(|err| {
                        format!("service account file at {path} is missing fields: {err}")
                    })?;
                fetch_oauth2_token(client, &key.client_email, &key.private_key).await
            }
            AdcKind::AuthorizedUser => {
                let field = |name: &str| {
                    document
                        .get(name)
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("credential file at {path} is missing {name}."))
                };
                fetch_refresh_token_grant(
                    client,
                    field("client_id")?,
                    field("client_secret")?,
                    field("refresh_token")?,
                )
                .await
            }
            // Workload identity federation: no key to read, only a subject
            // token from elsewhere that Google STS will exchange.
            AdcKind::ExternalAccount => match ExternalAccount::from_document(&document, &path) {
                Ok(account) => account.fetch_token(client, scope).await,
                Err(err) => Err(err),
            },
            AdcKind::Unknown(kind) => Err(format!(
                "the credential file at {path} has an unrecognised credential type {kind:?}."
            )),
        };
    }

    // No file anywhere: on GCE/GKE/Cloud Run the metadata server is the
    // credential source, and it is the reason ADC works with nothing configured.
    fetch_metadata_token(client, scope).await.map_err(|err| {
        if tried.is_empty() {
            err
        } else {
            format!("{err} (no credential file at: {})", tried.join(", "))
        }
    })
}

/// Ask the GCE metadata server for a token.
async fn fetch_metadata_token(client: &Client, scope: &str) -> Result<String, String> {
    let host = std::env::var("GCE_METADATA_HOST")
        .unwrap_or_else(|_| "metadata.google.internal".to_string());
    let url = format!(
        "http://{host}/computeMetadata/v1/instance/service-accounts/default/token?scopes={}",
        percent_encode(scope)
    );
    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .map_err(|_| {
            "no Google credentials found: set GOOGLE_APPLICATION_CREDENTIALS, run \
             `gcloud auth application-default login`, or supply a service account key."
                .to_string()
        })?;
    let text = response
        .text()
        .await
        .map_err(|err| format!("metadata token response read failed: {err}"))?;
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "the metadata server returned no access_token.".to_string())
}

/// Exchange a token for one belonging to another service account.
///
/// This is what `--impersonate-service-account` does: the caller keeps its own
/// identity and borrows the target's permissions, so nobody has to hold the
/// target's key.
async fn impersonate_service_account(
    client: &Client,
    source_token: &str,
    target: &str,
    scope: &str,
    delegates: &[String],
) -> Result<String, String> {
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        percent_encode(target)
    );
    generate_access_token(client, &url, source_token, scope, delegates).await
}

/// A workload identity federation credential (`type: external_account`).
///
/// This is how a workload outside Google proves who it is without a downloaded
/// key: some other system — a Kubernetes projected service account token, a
/// CI provider's OIDC token, an OS file — issues a *subject token*, and Google
/// STS exchanges it for an access token. The point of the whole mechanism is
/// that no long-lived private key is stored anywhere, so refusing to support it
/// pushes users back onto exactly the key files it exists to eliminate.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExternalAccount {
    audience: String,
    subject_token_type: String,
    token_url: String,
    source: SubjectTokenSource,
    /// Present when the federated identity is only a stepping stone to a
    /// service account, which is the common configuration.
    impersonation_url: Option<String>,
}

/// Where the subject token comes from.
///
/// Only the two sources that need nothing but this process are supported. The
/// `executable` source runs a user-named command, which Google itself gates
/// behind `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`; the AWS source signs a
/// GetCallerIdentity request. Both are refused by name below rather than
/// silently ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SubjectTokenSource {
    /// A file on disk, the shape Kubernetes projected tokens take.
    File { path: String, field: Option<String> },
    /// An HTTP endpoint, the shape most CI OIDC providers take.
    Url {
        url: String,
        headers: Vec<(String, String)>,
        field: Option<String>,
    },
}

impl ExternalAccount {
    /// Read the credential document. `Err` describes what is missing, because
    /// these files are generated by `gcloud` and hand-editing them is where
    /// they go wrong.
    fn from_document(document: &Value, path: &str) -> Result<Self, String> {
        let field = |name: &str| {
            document
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| format!("the credential file at {path} is missing {name}."))
        };
        let credential_source = document.get("credential_source").ok_or_else(|| {
            format!("the credential file at {path} is missing credential_source.")
        })?;

        // `format` says whether the token sits alone in the file or inside a
        // JSON document under a named field.
        let subject_field = credential_source
            .get("format")
            .filter(|format| format.get("type").and_then(Value::as_str) == Some("json"))
            .and_then(|format| format.get("subject_token_field_name"))
            .and_then(Value::as_str)
            .map(str::to_string);

        let source = if let Some(file) = credential_source.get("file").and_then(Value::as_str) {
            SubjectTokenSource::File {
                path: file.to_string(),
                field: subject_field,
            }
        } else if let Some(url) = credential_source.get("url").and_then(Value::as_str) {
            let headers = credential_source
                .get("headers")
                .and_then(Value::as_object)
                .map(|headers| {
                    headers
                        .iter()
                        .filter_map(|(name, value)| {
                            value
                                .as_str()
                                .map(|value| (name.clone(), value.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            SubjectTokenSource::Url {
                url: url.to_string(),
                headers,
                field: subject_field,
            }
        } else if credential_source.get("executable").is_some() {
            return Err(format!(
                "the credential file at {path} uses an executable-sourced subject token, \
                 which runs a command to obtain the credential. This connector does not \
                 run it. Point credential_source at a file or url instead."
            ));
        } else if credential_source.get("environment_id").is_some() {
            return Err(format!(
                "the credential file at {path} federates an AWS identity, which this \
                 connector cannot sign for. Use a file- or url-sourced subject token."
            ));
        } else {
            return Err(format!(
                "the credential file at {path} has a credential_source this connector \
                 does not recognise; it must name a file or a url."
            ));
        };

        Ok(Self {
            audience: field("audience")?,
            subject_token_type: field("subject_token_type")?,
            token_url: field("token_url")?,
            source,
            impersonation_url: document
                .get("service_account_impersonation_url")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    /// Exchange the subject token for a Google access token.
    async fn fetch_token(&self, client: &Client, scope: &str) -> Result<String, String> {
        let subject_token = self.read_subject_token(client).await?;

        // Impersonation needs the broad scope on the STS exchange and the
        // caller's scope on the impersonation call; without impersonation the
        // caller's scope goes on the exchange itself.
        let sts_scope = if self.impersonation_url.is_some() {
            "https://www.googleapis.com/auth/cloud-platform"
        } else {
            scope
        };
        let body = format!(
            "grant_type={}&audience={}&scope={}&requested_token_type={}\
             &subject_token={}&subject_token_type={}",
            percent_encode("urn:ietf:params:oauth:grant-type:token-exchange"),
            percent_encode(&self.audience),
            percent_encode(sts_scope),
            percent_encode("urn:ietf:params:oauth:token-type:access_token"),
            percent_encode(&subject_token),
            percent_encode(&self.subject_token_type),
        );
        let response = client
            .post(&self.token_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(|err| format!("Google STS token exchange failed: {err}"))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| format!("Google STS token response read failed: {err}"))?;
        if !status.is_success() {
            // The STS error body quotes the audience and can quote the subject
            // token, and this string reaches logs.
            return Err(format!(
                "Google STS returned HTTP {status} for the token exchange."
            ));
        }
        let federated = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|value| {
                value
                    .get("access_token")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| "the Google STS response contained no access_token.".to_string())?;

        match &self.impersonation_url {
            None => Ok(federated),
            // The document supplies the whole URL, so this goes straight to
            // the shared POST rather than rebuilding it from an email.
            Some(url) => generate_access_token(client, url, &federated, scope, &[]).await,
        }
    }

    async fn read_subject_token(&self, client: &Client) -> Result<String, String> {
        let (raw, origin) = match &self.source {
            SubjectTokenSource::File { path, .. } => (
                std::fs::read_to_string(path).map_err(|err| {
                    format!("the subject token file at {path} could not be read: {err}")
                })?,
                path.clone(),
            ),
            SubjectTokenSource::Url { url, headers, .. } => {
                let mut request = client.get(url);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request
                    .send()
                    .await
                    .map_err(|err| format!("the subject token request to {url} failed: {err}"))?;
                let status = response.status();
                let text = response.text().await.map_err(|err| {
                    format!("the subject token response from {url} could not be read: {err}")
                })?;
                if !status.is_success() {
                    return Err(format!(
                        "the subject token endpoint returned HTTP {status}."
                    ));
                }
                (text, url.clone())
            }
        };

        let field = match &self.source {
            SubjectTokenSource::File { field, .. } | SubjectTokenSource::Url { field, .. } => field,
        };
        let token = match field {
            None => raw.trim().to_string(),
            Some(field) => serde_json::from_str::<Value>(&raw)
                .ok()
                .and_then(|value| value.get(field).and_then(Value::as_str).map(str::to_string))
                .ok_or_else(|| {
                    format!("the subject token from {origin} has no {field:?} field.")
                })?,
        };
        if token.is_empty() {
            return Err(format!("the subject token from {origin} is empty."));
        }
        Ok(token)
    }
}

/// POST to an `iamcredentials` generateAccessToken endpoint.
///
/// Split out because the two callers arrive at the URL differently: an
/// explicitly configured impersonation target names a service account and the
/// URL is built from it, while a workload identity credential file supplies
/// the URL already assembled.
async fn generate_access_token(
    client: &Client,
    url: &str,
    source_token: &str,
    scope: &str,
    delegates: &[String],
) -> Result<String, String> {
    let body = serde_json::json!({
        "scope": [scope],
        "delegates": delegates
            .iter()
            .map(|d| format!("projects/-/serviceAccounts/{d}"))
            .collect::<Vec<_>>(),
    });
    let response = client
        .post(url)
        .bearer_auth(source_token)
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("impersonation request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("impersonation response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "impersonation failed with HTTP {status}. The caller needs \
             roles/iam.serviceAccountTokenCreator on the target service account."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("accessToken")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "impersonation response contained no accessToken.".to_string())
}

async fn fetch_oauth2_token(
    client: &Client,
    email: &str,
    private_key: &str,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now + 3600;
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let claims = format!(
        r#"{{"iss":"{}","scope":"https://www.googleapis.com/auth/bigquery","aud":"https://oauth2.googleapis.com/token","exp":{},"iat":{}}}"#,
        email, exp, now
    );
    let payload = format!(
        "{}.{}",
        base64_url_encode(header.as_bytes()),
        base64_url_encode(claims.as_bytes())
    );
    let signature = sign_rs256(private_key, payload.as_bytes())?;
    let assertion = format!("{payload}.{}", base64_url_encode(&signature));
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("GCP token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("GCP token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("GCP token request returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("GCP token JSON parse failed: {err}: {text}"))?;
    value
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "GCP token response missing access_token.".to_string())
}

fn sign_rs256(private_key: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    use ring::rand::SystemRandom;
    use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};

    let key = pem::parse(private_key)
        .map_err(|_| "invalid Google service account private key PEM.".to_string())?;
    if key.tag() != "PRIVATE KEY" {
        return Err("Google service account private key must use PKCS#8 PEM.".to_string());
    }
    let key_pair = RsaKeyPair::from_pkcs8(key.contents())
        .map_err(|_| "invalid Google service account PKCS#8 private key.".to_string())?;
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .map_err(|_| "Google service account JWT signing failed.".to_string())?;
    Ok(signature)
}

fn base64_url_encode(input: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as usize
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as usize
        } else {
            0
        };
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        if i + 1 < input.len() {
            out.push(CHARS[((b1 & 15) << 2) | (b2 >> 6)] as char);
        }
        if i + 2 < input.len() {
            out.push(CHARS[b2 & 63] as char);
        }
        i += 3;
    }
    out
}

fn connection(connection_id: &str) -> Result<BigQueryConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

fn field(columns: &[String], row: &[Value], name: &str) -> Option<String> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
        .and_then(|index| row.get(index))
        .and_then(|value| match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_base64_url_without_padding() {
        assert_eq!(base64_url_encode(b"abc"), "YWJj");
        assert_eq!(base64_url_encode(b"ab"), "YWI");
    }

    #[test]
    fn parses_token_config() {
        let request = json!({
            "profile": {
                "projectId": "project-a",
                "token": "ya29.token",
                "location": "US"
            }
        });
        let runtime = Runtime::new().unwrap();
        let config = runtime
            .block_on(BigQueryConfig::from_request(&request))
            .unwrap();
        assert_eq!(config.project_id, "project-a");
        assert_eq!(config.location.as_deref(), Some("US"));
    }

    #[test]
    fn recognises_each_application_default_credential_shape() {
        // ADC is three different files needing three different exchanges, and
        // the `type` field is the only thing that says which.
        assert_eq!(
            adc_kind(&json!({ "type": "service_account", "client_email": "a@b" })),
            AdcKind::ServiceAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "authorized_user", "refresh_token": "r" })),
            AdcKind::AuthorizedUser
        );
        assert_eq!(
            adc_kind(&json!({ "type": "external_account" })),
            AdcKind::ExternalAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "something_new" })),
            AdcKind::Unknown("something_new".to_string())
        );
        assert_eq!(adc_kind(&json!({})), AdcKind::Unknown(String::new()));
    }

    #[test]
    fn looks_for_credentials_where_gcloud_puts_them() {
        // Matching the Google client libraries' search order means a machine
        // already set up for `gcloud` needs no configuration here.
        assert_eq!(
            adc_paths_from(
                Some("/keys/explicit.json"),
                Some("/cfg/gcloud"),
                Some("/home/u")
            ),
            vec![
                "/keys/explicit.json".to_string(),
                "/cfg/gcloud/application_default_credentials.json".to_string(),
            ]
        );
        // Without CLOUDSDK_CONFIG the well-known path under HOME is used.
        assert_eq!(
            adc_paths_from(None, None, Some("/home/u")),
            vec!["/home/u/.config/gcloud/application_default_credentials.json".to_string()]
        );
        // Nothing to go on: the caller falls through to the metadata server.
        assert!(adc_paths_from(None, None, None).is_empty());
    }

    #[test]
    fn an_empty_credentials_variable_is_not_a_path() {
        // An exported-but-empty variable is common in shell profiles and would
        // otherwise send the search to "".
        assert_eq!(
            adc_paths_from(Some("   "), Some("/cfg"), None),
            vec!["/cfg/application_default_credentials.json".to_string()]
        );
    }

    #[test]
    fn form_encoding_protects_the_grant_body() {
        // A refresh token or a service account email in a form body must not be
        // able to introduce another parameter.
        assert_eq!(percent_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(
            percent_encode("svc@project.iam.gserviceaccount.com"),
            "svc%40project.iam.gserviceaccount.com"
        );
        assert_eq!(percent_encode("plain-Token_1.0~"), "plain-Token_1.0~");
    }

    #[test]
    fn a_kubernetes_style_credential_reads_its_token_from_a_file() {
        let account = ExternalAccount::from_document(
            &json!({
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/v",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": { "file": "/var/run/secrets/token" }
            }),
            "/creds.json",
        )
        .expect("credential");
        assert_eq!(
            account.source,
            SubjectTokenSource::File {
                path: "/var/run/secrets/token".into(),
                field: None
            }
        );
        assert_eq!(account.impersonation_url, None);
    }

    #[test]
    fn a_json_formatted_source_names_the_field_holding_the_token() {
        let account = ExternalAccount::from_document(
            &json!({
                "audience": "a", "subject_token_type": "t",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": {
                    "url": "https://ci.example/token",
                    "headers": { "Authorization": "Bearer ci" },
                    "format": { "type": "json", "subject_token_field_name": "value" }
                }
            }),
            "/creds.json",
        )
        .expect("credential");
        assert_eq!(
            account.source,
            SubjectTokenSource::Url {
                url: "https://ci.example/token".into(),
                headers: vec![("Authorization".into(), "Bearer ci".into())],
                field: Some("value".into()),
            }
        );
    }

    #[test]
    fn an_executable_source_is_refused_by_name() {
        // Google gates this behind an environment variable because it runs a
        // command from a credential file. Refusing it explicitly is the point:
        // a silent fallback to another credential would be worse.
        let err = ExternalAccount::from_document(
            &json!({
                "audience": "a", "subject_token_type": "t", "token_url": "u",
                "credential_source": { "executable": { "command": "/usr/bin/get-token" } }
            }),
            "/creds.json",
        )
        .unwrap_err();
        assert!(err.contains("executable"), "{err}");
        assert!(err.contains("file or url"), "{err}");
    }

    #[test]
    fn an_aws_federated_source_is_refused_by_name() {
        let err = ExternalAccount::from_document(
            &json!({
                "audience": "a", "subject_token_type": "t", "token_url": "u",
                "credential_source": { "environment_id": "aws1", "region_url": "http://169.254.169.254/x" }
            }),
            "/creds.json",
        )
        .unwrap_err();
        assert!(err.contains("AWS"), "{err}");
    }

    #[test]
    fn a_missing_field_is_named_rather_than_defaulted() {
        let err = ExternalAccount::from_document(
            &json!({ "audience": "a", "credential_source": { "file": "/t" } }),
            "/creds.json",
        )
        .unwrap_err();
        assert!(err.contains("subject_token_type"), "{err}");
        assert!(err.contains("/creds.json"), "{err}");
    }

    #[test]
    fn a_credential_source_with_neither_file_nor_url_is_refused() {
        let err = ExternalAccount::from_document(
            &json!({
                "audience": "a", "subject_token_type": "t", "token_url": "u",
                "credential_source": { "something_else": true }
            }),
            "/creds.json",
        )
        .unwrap_err();
        assert!(err.contains("file or a url"), "{err}");
    }

    #[test]
    fn impersonation_is_carried_through_when_the_document_asks_for_it() {
        let account = ExternalAccount::from_document(
            &json!({
                "audience": "a", "subject_token_type": "t",
                "token_url": "https://sts.googleapis.com/v1/token",
                "service_account_impersonation_url":
                    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/x@y.iam.gserviceaccount.com:generateAccessToken",
                "credential_source": { "file": "/var/run/secrets/token" }
            }),
            "/creds.json",
        )
        .expect("credential");
        assert!(account
            .impersonation_url
            .as_deref()
            .is_some_and(|url| url.ends_with(":generateAccessToken")));
    }

    #[test]
    fn a_plain_token_file_is_read_whole_and_trimmed() {
        let dir = std::env::temp_dir().join("irodori-external-account-test");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("token");
        std::fs::write(&path, "  header.payload.signature\n").expect("write");
        let account = ExternalAccount {
            audience: "a".into(),
            subject_token_type: "t".into(),
            token_url: "u".into(),
            source: SubjectTokenSource::File {
                path: path.to_string_lossy().into_owned(),
                field: None,
            },
            impersonation_url: None,
        };
        let token = runtime()
            .expect("runtime")
            .block_on(account.read_subject_token(&Client::new()))
            .expect("token");
        assert_eq!(token, "header.payload.signature");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn an_empty_token_file_is_an_error_rather_than_an_empty_credential() {
        let dir = std::env::temp_dir().join("irodori-external-account-test");
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("empty-token");
        std::fs::write(&path, "   \n").expect("write");
        let account = ExternalAccount {
            audience: "a".into(),
            subject_token_type: "t".into(),
            token_url: "u".into(),
            source: SubjectTokenSource::File {
                path: path.to_string_lossy().into_owned(),
                field: None,
            },
            impersonation_url: None,
        };
        let err = runtime()
            .expect("runtime")
            .block_on(account.read_subject_token(&Client::new()))
            .unwrap_err();
        assert!(err.contains("empty"), "{err}");
        std::fs::remove_file(&path).ok();
    }
}
