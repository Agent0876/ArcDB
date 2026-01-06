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
    rows?: Array<{
        values: any[];
    }>;
}
export declare class ArcClient extends EventEmitter {
    private socket;
    private config;
    private buffer;
    private pendingResolvers;
    private isReady;
    constructor(config?: ArcConfig);
    connect(): Promise<void>;
    private processBuffer;
    private handleMessage;
    query(sql: string): Promise<QueryResult>;
    close(): void;
}
