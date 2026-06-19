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

## Parameters (in order)

| # | Parameter | Description | Example |
|---|-----------|-------------|---------|
| 1 | `connStr` | MongoDB connection string | `mongodb://localhost:27017/testdb` |
| 2 | `threads` | Number of worker threads | `10` or `50` |
| 3 | `duration` | Insert duration in **minutes** (must be > 0) | `5` or `10` |
| 4 | `batchWait` | Wait between batches (ms) | `0` or `100` |
| 5 | `docsPerIter` | Documents per batch | `100` or `1000` |
| 6 | `collection` | Collection name | `testColl` |
| 7 | `useStringIds` | Use string IDs (true/false) | `false` |
| 8 | `enableUpdatePhase` | Run bulk update after inserts (true/false) | `true` |

**⚠️ Important:** Duration must be > 0 or no documents will be inserted!

## Examples

### High throughput test (Atlas)
```bash
java -jar target/perf-client-1.0-SNAPSHOT.jar \
  "mongodb+srv://user:pass@cluster.mongodb.net/testdb" \
  50 \
  5 \
  0 \
  1000 \
  collection \
  false \
  true
```
- 50 threads, 5 minutes, 1000 docs/batch, with update phase

### Insert only (Atlas)
```bash
java -jar target/perf-client-1.0-SNAPSHOT.jar \
  "mongodb+srv://user:pass@cluster.mongodb.net/testdb" \
  10 \
  5 \
  0 \
  100 \
  testColl \
  false \
  false
```
- 10 threads, 5 minutes, 100 docs/batch, no update phase

> Atlas Search indexes require MongoDB Atlas. Local MongoDB deployments cannot run this
> default workload because setup intentionally fails fast if the Search index cannot be
> created.

## What it does

1. **Cleanup**: Drops existing search index and collection
2. **Setup**: Creates collection and Atlas Search index (dynamic mapping)
3. **Wait**: Waits 60 seconds for index to be ready
4. **Phase 1 - Insert**: Multi-threaded inserts (30KB docs) for specified duration
5. **Phase 2 - Update** (optional): Bulk updates all docs, adding `value2` (~30KB) and `lastUpdated`
6. **Cleanup**: Drops search index and collection

## Many-index scale scenario

The jar entry point runs the single-collection workload above. A separate scenario is
available for testing many Atlas Search indexes with writes focused on a smaller active
set:

```bash
java -cp target/perf-client-1.0-SNAPSHOT.jar org.perf.ManyIndexesScenario \
  "mongodb+srv://user:pass@cluster.mongodb.net/testdb"
```

Optional JVM properties:

```bash
java \
  -DtotalCollections=100 \
  -DactiveCollections=2 \
  -DdurationMinutes=10 \
  -Dthreads=10 \
  -DdocsPerBatch=5000 \
  -DcleanupAfterRun=true \
  -cp target/perf-client-1.0-SNAPSHOT.jar \
  org.perf.ManyIndexesScenario \
  "mongodb+srv://user:pass@cluster.mongodb.net/testdb"
```

This scenario creates `scale_test_*` collections in the database from the connection
string, waits for all Search indexes to become queryable, and updates only the batch each
worker just inserted. It drops the scale-test collections at the end by default; set
`-DcleanupAfterRun=false` when you need to inspect the data after a run.

## Requirements

- Java 17+
- **MongoDB Atlas cluster** (Atlas Search indexes only work on Atlas, not local MongoDB)

