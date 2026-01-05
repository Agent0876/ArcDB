//! ArcDB - CLI Client

use std::io::{self, Write};
use std::sync::Arc;

use arcdb::catalog::Catalog;
use arcdb::executor::{ExecutionEngine, Planner};
use arcdb::sql::Parser;

/// Print welcome banner
fn print_banner() {
    println!(
        r#"
   _              ____  ____  
  / \   _ __ ___ |  _ \| __ ) 
 / _ \ | '__/ __|| | | |  _ \ 
/ ___ \| | | (__ | |_| | |_) |
/_/   \_\_|  \___||____/|____/ 

 A simple relational database engine in Rust
 Type '.help' for help, '.quit' to exit
"#
    );
}

/// Print help message
fn print_help() {
    println!(
        r#"
Commands:
  .help              Show this help message
  .quit              Exit ArcDB
  .tables            List all tables
  .schema <table>    Show table schema
  .clear             Clear screen

SQL Commands:
  CREATE TABLE ...   Create a new table
  DROP TABLE ...     Drop a table
  INSERT INTO ...    Insert rows
  SELECT ...         Query data
  UPDATE ...         Update rows
  DELETE FROM ...    Delete rows

Examples:
  CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));
  INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
  SELECT * FROM users WHERE id = 1;
"#
    );
}

/// Format query results as a table
fn format_results(columns: &[String], rows: &[arcdb::storage::Tuple]) -> String {
    if columns.is_empty() && rows.is_empty() {
        return String::new();
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();

    for row in rows {
        for (i, value) in row.values().iter().enumerate() {
            if i < widths.len() {
                let value_len = format!("{}", value).len();
                widths[i] = widths[i].max(value_len);
            }
        }
    }

    let mut output = String::new();

    // Header separator
    let separator: String = widths
        .iter()
        .map(|w| "-".repeat(*w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let separator = format!("+{}+\n", separator);

    // Header
    output.push_str(&separator);
    let header: String = columns
        .iter()
        .zip(&widths)
        .map(|(c, w)| format!(" {:^width$} ", c, width = *w))
        .collect::<Vec<_>>()
        .join("|");
    output.push_str(&format!("|{}|\n", header));
    output.push_str(&separator);

    // Rows
    for row in rows {
        let row_str: String = row
            .values()
            .iter()
            .zip(&widths)
            .map(|(v, w)| format!(" {:>width$} ", v, width = *w))
            .collect::<Vec<_>>()
            .join("|");
        output.push_str(&format!("|{}|\n", row_str));
    }

    if !rows.is_empty() {
        output.push_str(&separator);
    }

    output.push_str(&format!("{} row(s) returned\n", rows.len()));

    output
}

/// Execute a SQL statement
fn execute_sql(sql: &str, catalog: &Catalog, engine: &mut ExecutionEngine) {
    let sql = sql.trim();
    if sql.is_empty() {
        return;
    }

    // Parse
    let mut parser = match Parser::new(sql) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Parse error: {}", e);
            return;
        }
    };

    let stmt = match parser.parse() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Parse error: {}", e);
            return;
        }
    };

    // Plan
    let planner = Planner::new(catalog);
    let plan = planner.plan(stmt);

    // Execute
    match engine.execute(plan) {
        Ok(result) => {
            if let Some(msg) = result.message {
                println!("{}", msg);
            } else if !result.columns.is_empty() || !result.rows.is_empty() {
                print!("{}", format_results(&result.columns, &result.rows));
            } else if result.affected_rows > 0 {
                println!("{} row(s) affected", result.affected_rows);
            }
        }
        Err(e) => {
            eprintln!("Execution error: {}", e);
        }
    }
}

/// Handle special dot commands
fn handle_special_command(cmd: &str, catalog: &Catalog) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();

    match parts.first().map(|s| *s) {
        Some(".help") => print_help(),
        Some(".quit") | Some(".exit") => {
            catalog.save_to_disk("arcdb.meta").ok();
            println!("Goodbye!");
            std::process::exit(0);
        }
        Some(".tables") => {
            let tables = catalog.list_tables();
            if tables.is_empty() {
                println!("No tables found.");
            } else {
                println!("Tables:");
                for table in tables {
                    println!("  {}", table);
                }
            }
        }
        Some(".schema") => {
            if let Some(table_name) = parts.get(1) {
                match catalog.get_table_info(table_name) {
                    Ok(info) => println!("{}", info),
                    Err(e) => eprintln!("Error: {}", e),
                }
            } else {
                // Show schema for all tables
                for table_name in catalog.list_tables() {
                    match catalog.get_table_info(&table_name) {
                        Ok(info) => println!("{}", info),
                        Err(e) => eprintln!("Error: {}", e),
                    }
                }
            }
        }
        Some(".clear") => {
            // Clear screen (ANSI escape code)
            print!("\x1B[2J\x1B[1;1H");
            io::stdout().flush().unwrap();
        }
        Some(cmd) => {
            eprintln!("Unknown command: {}", cmd);
            eprintln!("Type '.help' for available commands.");
        }
        None => {}
    }
}

/// Main REPL loop
fn run_repl() {
    let catalog =
        Arc::new(Catalog::load_from_disk("arcdb.meta").unwrap_or_else(|_| Catalog::new()));
    let mut engine = ExecutionEngine::new(catalog.clone()).unwrap();

    print_banner();

    let mut input_buffer = String::new();
    let mut in_multiline = false;

    loop {
        // Print prompt
        let prompt = if in_multiline { "...> " } else { "arcdb> " };
        print!("{}", prompt);
        io::stdout().flush().unwrap();

        // Read line
        let mut line = String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0) => {
                catalog.save_to_disk("arcdb.meta").ok();
                break;
            } // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                continue;
            }
        }

        let trimmed = line.trim();

        // Handle special commands
        if !in_multiline && trimmed.starts_with('.') {
            handle_special_command(trimmed, &catalog);
            continue;
        }

        // Handle empty input
        if trimmed.is_empty() {
            if in_multiline {
                // Empty line in multiline mode - execute the buffer
                in_multiline = false;
                let sql = input_buffer.clone();
                input_buffer.clear();
                execute_sql(&sql, &catalog, &mut engine);
            }
            continue;
        }

        // Accumulate input
        input_buffer.push_str(&line);

        // Check if statement is complete (ends with semicolon)
        if trimmed.ends_with(';') {
            in_multiline = false;
            let sql = input_buffer.clone();
            input_buffer.clear();
            execute_sql(&sql, &catalog, &mut engine);
        } else {
            in_multiline = true;
        }
    }

    println!("\nGoodbye!");
}

fn main() {
    run_repl();
}
