"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ArcClient = void 0;
const net = __importStar(require("net"));
const events_1 = require("events");
class ArcClient extends events_1.EventEmitter {
    constructor(config = {}) {
        super();
        this.buffer = '';
        this.pendingResolvers = [];
        this.isReady = false;
        this.config = {
            host: config.host || '127.0.0.1',
            port: config.port || 7171,
        };
        this.socket = new net.Socket();
        this.socket.setEncoding('utf8');
    }
    async connect() {
        return new Promise((resolve, reject) => {
            this.socket.connect(this.config.port, this.config.host, () => {
                // Connection established, wait for welcome message
            });
            this.socket.on('data', (data) => {
                this.buffer += data.toString();
                this.processBuffer();
            });
            this.socket.on('error', (err) => {
                if (!this.isReady) {
                    reject(err);
                }
                else {
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
    processBuffer() {
        let newlineIndex;
        while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
            const line = this.buffer.substring(0, newlineIndex).trim();
            this.buffer = this.buffer.substring(newlineIndex + 1);
            if (line) {
                this.handleMessage(line);
            }
        }
    }
    handleMessage(line) {
        // Handshake handling
        if (!this.isReady) {
            // We ignore everything until we see evidence of our tokens or just treat the first response cycle as done.
            // For ArcDB, we expect:
            // 1. Welcome ...
            // 2. Ready ...
            // 3. Output mode set to JSON
            if (line.includes("Output mode set to JSON")) {
                const resolver = this.pendingResolvers.shift();
                if (resolver)
                    resolver.resolve(null);
            }
            // If we get an error during handshake
            if (line.startsWith("Error:") || line.includes("Unknown command")) {
                const resolver = this.pendingResolvers.shift();
                if (resolver)
                    resolver.reject(new Error(line));
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
                }
                else {
                    // Fallback for non-JSON lines (e.g. server info messages)
                    resolver.resolve({ status: 'info', message: line });
                }
            }
            catch (e) {
                resolver.reject(new Error(`Failed to parse server response: ${line}`));
            }
        }
    }
    async query(sql) {
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
    close() {
        this.socket.end();
        this.socket.destroy();
    }
}
exports.ArcClient = ArcClient;
