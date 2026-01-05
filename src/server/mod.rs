//! TCP Server for ArcDB
//!
//! This module implements a simple TCP server that allows remote clients
//! to connect and execute SQL queries.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::executor::{ExecutionEngine, Planner, QueryResult};
use crate::sql::Parser;

/// Default server port
pub const DEFAULT_PORT: u16 = 7171;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Host address to bind
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Maximum concurrent connections
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: DEFAULT_PORT,
            max_connections: 100,
        }
    }
}

impl ServerConfig {
    /// Create a new server config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the host address
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Get the bind address as a string
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Parsed connection URL components
/// Format: scheme://[username:password@]host[:port]/path
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionUrl {
    /// URL scheme (e.g., "arcdb", "postgres")
    pub scheme: String,
    /// Optional username
    pub username: Option<String>,
    /// Optional password
    pub password: Option<String>,
    /// Host address
    pub host: String,
    /// Optional port number
    pub port: Option<u16>,
    /// Path component
    pub path: String,
}

impl ConnectionUrl {
    /// Parse a connection URL string
    /// Format: scheme://[username:password@]host[:port]/path
    pub fn parse(url: &str) -> Result<Self> {
        // Split scheme
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| Error::Internal("Invalid URL: missing scheme".to_string()))?;

        // Check for auth (username:password@)
        let (auth, host_port_path) = if let Some(at_pos) = rest.find('@') {
            let (auth_part, after_at) = rest.split_at(at_pos);
            (Some(auth_part), &after_at[1..]) // Skip '@'
        } else {
            (None, rest)
        };

        // Parse username and password
        let (username, password) = if let Some(auth_str) = auth {
            if let Some((user, pass)) = auth_str.split_once(':') {
                (Some(user.to_string()), Some(pass.to_string()))
            } else {
                (Some(auth_str.to_string()), None)
            }
        } else {
            (None, None)
        };

        // Split host:port from path
        let (host_port, path) = if let Some(slash_pos) = host_port_path.find('/') {
            let (hp, p) = host_port_path.split_at(slash_pos);
            (hp, &p[1..]) // Skip leading '/'
        } else {
            (host_port_path, "")
        };

        // Parse host and port
        let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
            // Check if this is IPv6 (contains '[')
            if h.contains('[') {
                (host_port.to_string(), None)
            } else {
                let port_num = p
                    .parse::<u16>()
                    .map_err(|_| Error::Internal(format!("Invalid port: {}", p)))?;
                (h.to_string(), Some(port_num))
            }
        } else {
            (host_port.to_string(), None)
        };

        Ok(Self {
            scheme: scheme.to_string(),
            username,
            password,
            host,
            port,
            path: path.to_string(),
        })
    }

    /// Convert to ServerConfig (ignoring auth and path)
    pub fn to_server_config(&self) -> ServerConfig {
        ServerConfig {
            host: self.host.clone(),
            port: self.port.unwrap_or(DEFAULT_PORT),
            max_connections: 100,
        }
    }
}

/// ArcDB TCP Server
pub struct Server {
    config: ServerConfig,
    catalog: Arc<Catalog>,
}

impl Server {
    /// Create a new server
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            catalog: Arc::new(Catalog::new()),
        }
    }

    /// Start the server and listen for connections
    pub fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind_address())?;

        println!("ArcDB server listening on {}", self.config.bind_address());
        println!("Press Ctrl+C to stop the server\n");

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let catalog = self.catalog.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, catalog) {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }

        Ok(())
    }
}

/// Output format for query results
#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Table,
    Json,
}

/// Handle a client connection
fn handle_connection(stream: TcpStream, catalog: Arc<Catalog>) -> Result<()> {
    let peer_addr = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    println!("Client connected: {}", peer_addr);

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // Create execution engine for this connection
    let mut engine = ExecutionEngine::new(catalog.clone())?;
    let mut format = OutputFormat::Table;

    // Send welcome message
    send_response(&mut writer, "ArcDB Server v0.1.0\nReady for queries.\n")?;

    // Read and execute queries
    let mut line = String::new();
    loop {
        line.clear();

        match reader.read_line(&mut line) {
            Ok(0) => {
                // EOF - client disconnected
                println!("Client disconnected: {}", peer_addr);
                break;
            }
            Ok(_) => {
                let query = line.trim();

                if query.is_empty() {
                    continue;
                }

                // Handle special commands
                if query.starts_with('.') {
                    match query {
                        ".quit" | ".exit" => {
                            send_response(&mut writer, "Goodbye!\n")?;
                            break;
                        }
                        ".mode json" => {
                            format = OutputFormat::Json;
                            send_response(&mut writer, "Output mode set to JSON\n")?;
                            continue;
                        }
                        ".mode table" => {
                            format = OutputFormat::Table;
                            send_response(&mut writer, "Output mode set to Table\n")?;
                            continue;
                        }
                        ".tables" => {
                            let tables = catalog.list_tables();
                            if tables.is_empty() {
                                send_response(&mut writer, "No tables found.\n")?;
                            } else {
                                let msg = format!(
                                    "Tables:\n{}\n",
                                    tables
                                        .iter()
                                        .map(|t| format!("  {}", t))
                                        .collect::<Vec<_>>()
                                        .join("\n")
                                );
                                send_response(&mut writer, &msg)?;
                            }
                            continue;
                        }
                        _ => {
                            send_response(&mut writer, &format!("Unknown command: {}\n", query))?;
                            continue;
                        }
                    }
                }

                // Execute SQL query
                let response = execute_query(&mut engine, &catalog, query, format);
                send_response(&mut writer, &response)?;
            }
            Err(e) => {
                eprintln!("Read error from {}: {}", peer_addr, e);
                break;
            }
        }
    }

    Ok(())
}

/// Execute a SQL query and return the response
fn execute_query(
    engine: &mut ExecutionEngine,
    catalog: &Catalog,
    sql: &str,
    format: OutputFormat,
) -> String {
    // Parse
    let mut parser = match Parser::new(sql) {
        Ok(p) => p,
        Err(e) => return format!("Parse error: {}\n", e),
    };

    let stmt = match parser.parse() {
        Ok(s) => s,
        Err(e) => return format!("Parse error: {}\n", e),
    };

    // Plan
    let planner = Planner::new(catalog);
    let plan = planner.plan(stmt);

    // Execute
    match engine.execute(plan) {
        Ok(result) => format_result(&result, format),
        Err(e) => format!("Execution error: {}\n", e),
    }
}

/// Format query result for sending to client
fn format_result(result: &QueryResult, format: OutputFormat) -> String {
    if let Some(ref msg) = result.message {
        // If JSON mode, wrap message in JSON object
        if format == OutputFormat::Json {
            return serde_json::json!({
                "status": "success",
                "message": msg,
                "affected_rows": result.affected_rows
            })
            .to_string()
                + "\n";
        }
        return format!("{}\n", msg);
    }

    if result.columns.is_empty() && result.rows.is_empty() {
        if format == OutputFormat::Json {
            return serde_json::json!({
                "status": "success",
                "affected_rows": result.affected_rows
            })
            .to_string()
                + "\n";
        }

        if result.affected_rows > 0 {
            return format!("{} row(s) affected\n", result.affected_rows);
        }
        return "OK\n".to_string();
    }

    // JSON Format
    if format == OutputFormat::Json {
        match serde_json::to_string(result) {
            Ok(json) => return json + "\n",
            Err(e) => {
                return format!(
                    "{{\"status\":\"error\",\"message\":\"Serialization error: {}\"}}\n",
                    e
                )
            }
        }
    }

    // Table Format (Default)
    let mut output = String::new();

    // Calculate column widths
    let mut widths: Vec<usize> = result.columns.iter().map(|c: &String| c.len()).collect();
    for row in &result.rows {
        for (i, value) in row.values().iter().enumerate() {
            if i < widths.len() {
                let value_len = format!("{}", value).len();
                widths[i] = widths[i].max(value_len);
            }
        }
    }

    // Header
    let separator: String = widths
        .iter()
        .map(|w| "-".repeat(*w + 2))
        .collect::<Vec<String>>()
        .join("+");

    output.push_str(&format!("+{}+\n", separator));

    let header: String = result
        .columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!(" {:^width$} ", c, width = *w))
        .collect::<Vec<String>>()
        .join("|");
    output.push_str(&format!("|{}|\n", header));
    output.push_str(&format!("+{}+\n", separator));

    // Rows
    for row in &result.rows {
        let row_str: String = row
            .values()
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!(" {:>width$} ", v, width = *w))
            .collect::<Vec<String>>()
            .join("|");
        output.push_str(&format!("|{}|\n", row_str));
    }

    if !result.rows.is_empty() {
        output.push_str(&format!("+{}+\n", separator));
    }

    output.push_str(&format!("{} row(s) returned\n", result.rows.len()));
    output
}

/// Send a response to the client
fn send_response(writer: &mut TcpStream, message: &str) -> Result<()> {
    writer.write_all(message.as_bytes())?;
    writer.flush()?;
    Ok(())
}

/// Simple client for testing
pub fn connect(host: &str, port: u16) -> Result<TcpStream> {
    let addr = format!("{}:{}", host, port);
    TcpStream::connect(&addr).map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config() {
        let config = ServerConfig::new().host("0.0.0.0").port(5500);

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 5500);
        assert_eq!(config.bind_address(), "0.0.0.0:5500");
    }

    #[test]
    fn test_format_result() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
            affected_rows: 0,
            message: Some("Table created".to_string()),
        };

        let output = format_result(&result, OutputFormat::Table);
        assert!(output.contains("Table created"));
    }

    #[test]
    fn test_connection_url_full() {
        let url = ConnectionUrl::parse("arcdb://admin:secret@localhost:5432/mydb").unwrap();
        assert_eq!(url.scheme, "arcdb");
        assert_eq!(url.username, Some("admin".to_string()));
        assert_eq!(url.password, Some("secret".to_string()));
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, Some(5432));
        assert_eq!(url.path, "mydb");
    }

    #[test]
    fn test_connection_url_no_auth() {
        let url = ConnectionUrl::parse("postgres://localhost:5432/testdb").unwrap();
        assert_eq!(url.scheme, "postgres");
        assert_eq!(url.username, None);
        assert_eq!(url.password, None);
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, Some(5432));
        assert_eq!(url.path, "testdb");
    }

    #[test]
    fn test_connection_url_no_port() {
        let url = ConnectionUrl::parse("arcdb://user:pass@myhost/data").unwrap();
        assert_eq!(url.scheme, "arcdb");
        assert_eq!(url.username, Some("user".to_string()));
        assert_eq!(url.password, Some("pass".to_string()));
        assert_eq!(url.host, "myhost");
        assert_eq!(url.port, None);
        assert_eq!(url.path, "data");
    }

    #[test]
    fn test_connection_url_minimal() {
        let url = ConnectionUrl::parse("http://example.com/path").unwrap();
        assert_eq!(url.scheme, "http");
        assert_eq!(url.username, None);
        assert_eq!(url.password, None);
        assert_eq!(url.host, "example.com");
        assert_eq!(url.port, None);
        assert_eq!(url.path, "path");
    }

    #[test]
    fn test_connection_url_username_only() {
        let url = ConnectionUrl::parse("arcdb://admin@localhost:3306/db").unwrap();
        assert_eq!(url.username, Some("admin".to_string()));
        assert_eq!(url.password, None);
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, Some(3306));
    }
}
