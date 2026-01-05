# Build Stage
FROM rust:1.75-slim-bookworm as builder

WORKDIR /usr/src/arcdb
COPY . .

# Build the release binary
RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /usr/src/arcdb/target/release/arcdb-server /app/arcdb-server

# Expose the default port
EXPOSE 7171

# Run the server
CMD ["./arcdb-server"]
