"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("./index");
async function main() {
    console.log("Connecting to ArcDB...");
    const client = new index_1.ArcClient({ port: 7171 });
    try {
        await client.connect();
        console.log("Connected!");
        console.log("Creating table...");
        let res = await client.query("CREATE TABLE node_test (id INTEGER PRIMARY KEY, msg VARCHAR(50));");
        console.log("Create result:", res);
        console.log("Inserting data...");
        res = await client.query("INSERT INTO node_test VALUES (1, 'Hello from Node'), (2, 'ArcDB is cool');");
        console.log("Insert result:", res);
        console.log("Querying data...");
        res = await client.query("SELECT * FROM node_test;");
        console.log("Select result:", JSON.stringify(res, null, 2));
    }
    catch (e) {
        console.error("Test failed:", e);
    }
    finally {
        client.close();
    }
}
main();
