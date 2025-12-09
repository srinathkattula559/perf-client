# MongoDB Performance Testing Client

Multi-threaded Java tool for MongoDB performance testing.

## Build

```bash
./mvnw clean package
```

## Run

```bash
java -jar target/perf-client-1.0-SNAPSHOT.jar \
  <connStr> <threads> <duration> <batchWait> <docsPerIter> <collection> <useStringIds> <enableUpdatePhase>
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connStr` | MongoDB connection string | `mongodb://localhost:27017/testdb` |
| `threads` | Number of worker threads | `10` |
| `duration` | Insert duration in minutes | `5` |
| `batchWait` | Wait between batches (ms) | `0` or `100` |
| `docsPerIter` | Documents per batch | `100` |
| `collection` | Collection name | `testColl` |
| `useStringIds` | Use string IDs (true/false) | `false` |
| `enableUpdatePhase` | Run bulk update after inserts (true/false) | `true` |

## Examples

### Insert only
```bash
java -jar target/perf-client-1.0-SNAPSHOT.jar \
  mongodb://localhost:27017/testdb 10 5 0 100 testColl false false
```

### Insert + bulk update
```bash
java -jar target/perf-client-1.0-SNAPSHOT.jar \
  mongodb://localhost:27017/testdb 10 5 0 100 testColl false true
```

## What it does

1. **Cleanup**: Drops existing search index and collection
2. **Setup**: Creates collection and Atlas Search index (dynamic mapping)
3. **Wait**: Waits 60 seconds for index to be ready
4. **Phase 1 - Insert**: Multi-threaded inserts (30KB docs) for specified duration
5. **Phase 2 - Update** (optional): Bulk updates all docs, adding `value2` (30KB) and `lastUpdated`
6. **Cleanup**: Drops search index and collection

## Requirements

- Java 17+
- **MongoDB Atlas cluster** (Atlas Search indexes only work on Atlas, not local MongoDB)

