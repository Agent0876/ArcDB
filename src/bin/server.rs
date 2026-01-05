use arcdb::server::{Server, ServerConfig};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut config = ServerConfig::new();

    // Simple argument parsing
    for i in 1..args.len() {
        if args[i] == "--port" || args[i] == "-p" {
            if let Some(port_str) = args.get(i + 1) {
                if let Ok(port) = port_str.parse() {
                    config = config.port(port);
                }
            }
        }
    }

    println!("Starting ArcDB Server...");
    let server = Server::new(config);
    if let Err(e) = server.start() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
