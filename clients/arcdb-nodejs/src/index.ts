import * as net from 'net';
import { EventEmitter } from 'events';

export interface ArcConfig {
    host?: string;
    port?: number;
}

export interface QueryResult {
    status: 'success' | 'error';
    message?: string;
    affected_rows?: number;
    columns?: string[];
    rows?: Array<{ values: any[] }>;
}

export class ArcClient extends EventEmitter {
    private socket: net.Socket;
    private config: ArcConfig;
    private buffer: string = '';
    private pendingResolvers: Array<{ resolve: (val: any) => void; reject: (err: any) => void }> = [];
    private isReady: boolean = false;

    constructor(config: ArcConfig = {}) {
        super();
        this.config = {
            host: config.host || '127.0.0.1',
            port: config.port || 7171,
        };
        this.socket = new net.Socket();
        this.socket.setEncoding('utf8');
    }

    public async connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.socket.connect(this.config.port!, this.config.host!, () => {
                // Connection established, wait for welcome message
            });

            this.socket.on('data', (data) => {
                this.buffer += data.toString();
                this.processBuffer();
            });

            this.socket.on('error', (err) => {
                if (!this.isReady) {
                    reject(err);
                } else {
                    this.emit('error', err);
                }
            });

            this.socket.on('close', () => {
                this.emit('close');
            });

            // Handshake logic
            // 1. We expect a welcome message.
            // 2. We send ".mode json".
            // 3. We expect confirmation.
            // Note: Since we are using a simple TCP stream, we'll cheat a bit and just fire ".mode json" immediately
            // and treat the first few responses as setup.

            // To make it robust:
            // We'll send the mode command right after connect.
            this.socket.write(".mode json\n");

            // We assume after a short delay or first data/response we are ready.
            // A better real-world protocol would have explicit handshakes, but for this:
            // The server sends "ArcDB Server...\nReady...\n" then "Output mode set to JSON\n"
            // We can just consume the buffer until we see that confirmation or just resolve after a short time.

            // Simpler approach: Queue a specific resolver for the handshake
            this.pendingResolvers.push({
                resolve: () => {
                    this.isReady = true;
                    resolve();
                },
                reject: reject
            });
        });
    }

    private processBuffer() {
        let newlineIndex;
        while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
            const line = this.buffer.substring(0, newlineIndex).trim();
            this.buffer = this.buffer.substring(newlineIndex + 1);

            if (line) {
                this.handleMessage(line);
            }
        }
    }

    private handleMessage(line: string) {
        // Handshake handling
        if (!this.isReady) {
            // We ignore everything until we see evidence of our tokens or just treat the first response cycle as done.
            // For ArcDB, we expect:
            // 1. Welcome ...
            // 2. Ready ...
            // 3. Output mode set to JSON
            if (line.includes("Output mode set to JSON")) {
                const resolver = this.pendingResolvers.shift();
                if (resolver) resolver.resolve(null);
            }
            // If we get an error during handshake
            if (line.startsWith("Error:") || line.includes("Unknown command")) {
                const resolver = this.pendingResolvers.shift();
                if (resolver) resolver.reject(new Error(line));
            }
            return;
        }

        // Query Handling
        const resolver = this.pendingResolvers.shift();
        if (resolver) {
            try {
                // Try parsing as JSON
                // ArcDB JSON format: { "status": "...", ... }
                // OR raw strings if something went wrong outside JSON mode (shouldn't happen if handshake worked)
                if (line.startsWith('{')) {
                    const result = JSON.parse(line);
                    resolver.resolve(result);
                } else {
                    // Fallback for non-JSON lines (e.g. server info messages)
                    resolver.resolve({ status: 'info', message: line });
                }
            } catch (e) {
                resolver.reject(new Error(`Failed to parse server response: ${line}`));
            }
        }
    }

    public async query(sql: string): Promise<QueryResult> {
        if (!this.isReady) {
            throw new Error("Client not connected");
        }

        return new Promise((resolve, reject) => {
            this.pendingResolvers.push({ resolve, reject });
            // ArcDB expects single-line SQL ended with newline.
            // We ensure it ends with newline.
            const command = sql.trim().replace(/\n/g, ' ') + '\n';
            this.socket.write(command);
        });
    }

    public close() {
        this.socket.end();
        this.socket.destroy();
    }
}
