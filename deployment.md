# ArcDB Deployment Guide

This guide explains how to package and deploy the ArcDB server and the Kotlin JDBC driver.

## 1. ArcDB Server (Docker)

We provide a `Dockerfile` to build a lightweight, self-contained Docker image for the ArcDB server.

### Build the Image
```bash
docker build -t arcdb:latest .
```
*Note: This might take a few minutes as it compiles the Rust project in release mode.*

### Run the Container
```bash
docker run -d -p 7171:7171 --name arcdb-server arcdb:latest
```

The server will be available at `localhost:7171`.

### Verify connection
You can check logs:
```bash
docker logs -f arcdb-server
```

## 2. Kotlin JDBC Driver

The JDBC driver is built with Gradle and can be published to your local Maven repository for use in other projects.

### Publish to Maven Local
```bash
cd clients/arcdb-jdbc
./gradlew publishToMavenLocal
```

### Use in Another Project
Add the dependency to your `build.gradle.kts`:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("com.arcdb:arcdb-jdbc:0.1.0")
}
```

## 3. Connecting

Use the standard JDBC URL format:
```
jdbc:arcdb://localhost:7171/mydb
```

## 4. Automated Cross-Platform Releases

We use GitHub Actions to automatically build ArcDB for Linux, macOS (Intel & Silicon), and Windows.

### Triggering a Release
Push a tag starting with `v` to the repository:

```bash
git tag v0.1.0
git push origin v0.1.0
```

### Artifacts
The workflow will run and produce the following binaries in the GitHub Releases page:
- `arcdb-linux-amd64`
- `arcdb-macos-amd64`
- `arcdb-macos-arm64`
- `arcdb-windows-amd64.exe`
