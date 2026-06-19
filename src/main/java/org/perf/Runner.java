package org.perf;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

public class Runner {

  private static final String DEFAULT_DB_NAME = "db1";
  private static final String SEARCH_INDEX_NAME = "default";
  private static final Duration INDEX_READY_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration INDEX_POLL_INTERVAL = Duration.ofSeconds(10);
  private static final int PAYLOAD_REPEAT_COUNT = 10_240; // ~30 KB for "foo".repeat(...)

  // Configuration
  private final String connectionString;
  private final int threadsCount;
  private final int durationMinutes;
  private final int docsPerIteration;
  private final long batchWaitMillis;
  private final String collectionName;
  private final boolean useStringIds;
  private final boolean enableUpdatePhase;
  private final String dbName;

  // Infrastructure
  private MongoClient mongoClient;
  private Thread[] threads;
  private ExecutorService backgroundGenPool; // Dedicated pool for data prep

  // Metrics (High concurrency counters)
  private final LongAdder totalInserted = new LongAdder();
  private final LongAdder totalUpdated = new LongAdder();
  private final LongAdder totalErrors = new LongAdder();

  // Timing
  private Instant insertStartTime;
  private Instant insertEndTime;

  public Runner(
      String connectionString,
      int threadsCount,
      int durationMinutes,
      long batchWaitMillis,
      int docsPerIteration,
      String collectionName,
      boolean useStringIds,
      boolean enableUpdatePhase) {

    validateConfig(
        connectionString,
        threadsCount,
        durationMinutes,
        batchWaitMillis,
        docsPerIteration,
        collectionName);

    this.connectionString = connectionString;
    this.threadsCount = threadsCount;
    this.durationMinutes = durationMinutes;
    this.docsPerIteration = docsPerIteration;
    this.batchWaitMillis = batchWaitMillis;
    this.collectionName = collectionName;
    this.useStringIds = useStringIds;
    this.enableUpdatePhase = enableUpdatePhase;

    // Parse DB name from connection string or default
    ConnectionString cs = new ConnectionString(connectionString);
    this.dbName = (cs.getDatabase() != null) ? cs.getDatabase() : DEFAULT_DB_NAME;
  }

  public void start() {
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(this.connectionString))
        .applyToSocketSettings(builder -> builder
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.MILLISECONDS))
        .applyToConnectionPoolSettings(builder -> builder
            .maxSize(Math.max(100, threadsCount * 2)))
        .build();

    this.mongoClient = MongoClients.create(settings);

    // Use a dedicated pool for background data generation to avoid starving the commonPool
    this.backgroundGenPool = Executors.newCachedThreadPool();

    this.threads = new Thread[this.threadsCount];

    System.out.printf("Client connected to DB '%s'.%n", dbName);

    try {
      // Step 1: Cleanup existing search indexes and collection
      cleanupBeforeTest();

      // Step 2: Create collection and Atlas Search index
      createSearchIndex();

      // Step 3: Wait for index to be ready
      waitForIndexReady();

      // Phase 1: Insert phase
      System.out.println("\n=== Phase 1: INSERT ===");
      System.out.printf("Spawning %d threads...%n", threadsCount);
      this.insertStartTime = Instant.now();
      this.insertEndTime = insertStartTime.plus(Duration.ofMinutes(durationMinutes));

      for (int i = 0; i < this.threadsCount; i++) {
        final int workerId = i;
        threads[i] = new Thread(() -> doInsertWork(workerId, insertEndTime));
        threads[i].setName("Insert-Worker-" + i);
        threads[i].start();
      }
    } catch (RuntimeException e) {
      cleanupAfterTest();
      shutdown();
      throw e;
    }
  }

  public void waitAndJoin() {
    try {
      // Wait for insert phase to complete
      for (Thread thread : threads) {
        if (thread != null) thread.join();
      }

      printInsertPhaseSummary();

      // Phase 2: Update phase (if enabled)
      if (enableUpdatePhase) {
        System.out.println("\n=== Phase 2: UPDATE ===");
        Instant updateStartTime = Instant.now();

        // Perform bulk update of all documents
        performBulkUpdate();

        Instant updatePhaseEnd = Instant.now();
        printUpdatePhaseSummary(updateStartTime, updatePhaseEnd);
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Interrupted during join");
    } catch (RuntimeException e) {
      totalErrors.add(1);
      System.err.println("Workload failed: " + e.getMessage());
      throw e;
    } finally {
      printFinalSummary();
      System.out.printf(
          "Leaving collection '%s' and search index '%s' in place for inspection.%n",
          collectionName, SEARCH_INDEX_NAME);
      shutdown();
    }
  }

  private void cleanupBeforeTest() {
    System.out.println("\nCleaning up existing search indexes and collections...");
    MongoDatabase db = mongoClient.getDatabase(dbName);

    dropAllSearchIndexes(db.getCollection(collectionName));

    // Drop collection
    try {
      db.getCollection(collectionName).drop();
      System.out.println("Dropped existing collection");
    } catch (Exception e) {
      System.out.printf("Collection does not exist: %s%n", e.getMessage());
    }
  }

  private void dropAllSearchIndexes(MongoCollection<Document> collection) {
    try {
      List<String> indexNames = new ArrayList<>();
      for (Document index : collection.listSearchIndexes()) {
        String name = index.getString("name");
        if (name != null && !name.isBlank()) {
          indexNames.add(name);
        }
      }

      if (indexNames.isEmpty()) {
        System.out.println("No existing search indexes found");
        return;
      }

      for (String indexName : indexNames) {
        collection.dropSearchIndex(indexName);
        System.out.printf("Dropped search index: %s%n", indexName);
      }
    } catch (Exception e) {
      System.out.printf("Could not list or drop search indexes: %s%n", e.getMessage());
    }
  }

  private void createSearchIndex() {
    System.out.println("\nCreating Atlas Search index...");
    MongoDatabase db = mongoClient.getDatabase(dbName);

    // Create collection
    try {
      db.createCollection(collectionName);
      System.out.printf("Created collection: %s%n", collectionName);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create collection " + collectionName, e);
    }

    // Create search index with dynamic mapping
    Document indexDefinition = new Document("mappings", new Document("dynamic", true));

    try {
      db.getCollection(collectionName).createSearchIndex(SEARCH_INDEX_NAME, indexDefinition);
      System.out.printf("Created Atlas Search index: %s%n", SEARCH_INDEX_NAME);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to create Atlas Search index. Atlas Search indexes require MongoDB Atlas.", e);
    }
  }

  private void waitForIndexReady() {
    System.out.println("Waiting for search index to be ready...");
    MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
    Instant deadline = Instant.now().plus(INDEX_READY_TIMEOUT);

    while (Instant.now().isBefore(deadline)) {
      for (Document index : collection.listSearchIndexes()) {
        if (!SEARCH_INDEX_NAME.equals(index.getString("name"))) {
          continue;
        }

        String status = index.getString("status");
        Object queryable = index.get("queryable");
        if ("READY".equalsIgnoreCase(status) || Boolean.TRUE.equals(queryable)) {
          System.out.printf("Search index %s is ready.%n", SEARCH_INDEX_NAME);
          return;
        }

        System.out.printf("Search index status: %s%n", status);
      }

      try {
        Thread.sleep(INDEX_POLL_INTERVAL.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for search index readiness", e);
      }
    }

    throw new IllegalStateException(
        "Timed out waiting for search index " + SEARCH_INDEX_NAME + " to become ready");
  }

  private void cleanupAfterTest() {
    System.out.println("\nCleaning up search index and collection...");
    MongoDatabase db = mongoClient.getDatabase(dbName);

    // Drop search index
    try {
      db.getCollection(collectionName).dropSearchIndex(SEARCH_INDEX_NAME);
      System.out.println("Dropped search index");
    } catch (Exception e) {
      System.out.printf("Error dropping index: %s%n", e.getMessage());
    }

    // Drop collection
    try {
      db.getCollection(collectionName).drop();
      System.out.println("Dropped collection");
    } catch (Exception e) {
      System.out.printf("Error dropping collection: %s%n", e.getMessage());
    }

    System.out.println("Clean up complete.");
  }

  private void shutdown() {
    if (backgroundGenPool != null) {
      backgroundGenPool.shutdownNow();
    }
    if (mongoClient != null) {
      mongoClient.close();
      System.out.println("MongoClient closed.");
    }
  }

  private void performBulkUpdate() {
    MongoDatabase db = mongoClient.getDatabase(dbName);
    long totalDocs = totalInserted.sum();
    System.out.printf("Starting bulk update of %,d documents...%n", totalDocs);
    System.out.println("(This may take several minutes with Atlas Search index enabled)");

    long startTime = System.currentTimeMillis();

    // Add another large field so the update phase measures index maintenance on document growth.
    Document updateDoc = new Document("$set",
        new Document("value2", "bar".repeat(PAYLOAD_REPEAT_COUNT))
            .append("lastUpdated", new java.util.Date())
    );

    Thread progressMonitor = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Thread.sleep(5000); // Print every 5 seconds
          long elapsed = (System.currentTimeMillis() - startTime) / 1000;
          System.out.printf("Update in progress... (%d seconds elapsed)%n", elapsed);
        }
      } catch (InterruptedException e) {
        // Expected when update completes
      }
    });
    progressMonitor.setDaemon(true);
    progressMonitor.start();

    try {
      // Update all documents in the collection
      var result = db.getCollection(collectionName).updateMany(
          new Document(), // Empty filter = all documents
          updateDoc
      );

      long endTime = System.currentTimeMillis();
      double durationSeconds = (endTime - startTime) / 1000.0;

      totalUpdated.add(result.getModifiedCount());

      double throughput = durationSeconds > 0 ? result.getModifiedCount() / durationSeconds : 0;
      System.out.printf(
          "Bulk update completed: %,d matched, %,d modified in %.1f seconds (%.0f docs/sec)%n",
          result.getMatchedCount(), result.getModifiedCount(), durationSeconds, throughput);
    } finally {
      progressMonitor.interrupt();
    }
  }

  private void printInsertPhaseSummary() {
    System.out.println("\n=== Insert Phase Complete ===");
    System.out.printf("Total Docs Inserted: %,d%n", totalInserted.sum());
    System.out.printf("Total Errors:        %,d%n", totalErrors.sum());

    // Calculate actual elapsed time, not configured duration
    double totalSeconds = Duration.between(insertStartTime, Instant.now()).toMillis() / 1000.0;
    double throughput = totalInserted.sum() / totalSeconds;
    System.out.printf("Duration:            %.1f seconds%n", totalSeconds);
    System.out.printf("Avg Throughput:      %.2f docs/sec%n", throughput);
    System.out.println("==============================");
  }

  private void printUpdatePhaseSummary(Instant startTime, Instant endTime) {
    double durationSeconds = Duration.between(startTime, endTime).toMillis() / 1000.0;
    double throughput = totalUpdated.sum() / durationSeconds;

    System.out.println("\n=== Update Phase Complete ===");
    System.out.printf("Total Docs Updated:  %,d%n", totalUpdated.sum());
    System.out.printf("Duration:            %.1f seconds%n", durationSeconds);
    System.out.printf("Avg Throughput:      %.2f docs/sec%n", throughput);
    System.out.println("==============================");
  }

  private void printFinalSummary() {
    System.out.println("\n=== Final Summary ===");
    System.out.printf("Total Docs Inserted: %,d%n", totalInserted.sum());
    System.out.printf("Total Docs Updated:  %,d%n", totalUpdated.sum());
    System.out.printf("Total Errors:        %,d%n", totalErrors.sum());
    System.out.println("=====================");
  }

  private void doInsertWork(int workerId, Instant endTime) {
    MongoDatabase db = mongoClient.getDatabase(dbName);

    // Pass workerId to ensure unique IDs across threads
    DocPrep prep = new DocPrep(docsPerIteration, useStringIds, workerId);

    // Initial fill (blocking)
    try {
      prep.refill().get();
    } catch (Exception e) {
      System.err.println("Worker " + workerId + " failed initial refill: " + e.getMessage());
      return;
    }

    long lastReportTime = System.currentTimeMillis();

    while (Instant.now().isBefore(endTime)) {
      try {
        // Swap batches
        List<Document> batch = prep.getDocs();

        // Trigger next refill asynchronously on dedicated pool
        CompletableFuture<Void> nextBatch = prep.refill(backgroundGenPool);

        // Insert (The actual work)
        db.getCollection(collectionName).insertMany(batch, new InsertManyOptions().ordered(false));
        totalInserted.add(batch.size());

        // Progress reporting every 10 seconds (only from worker 0 to avoid spam)
        if (workerId == 0) {
          long currentTime = System.currentTimeMillis();
          if (currentTime - lastReportTime >= 10000) {
            long totalDocs = totalInserted.sum();
            System.out.printf("Insert progress: %,d docs inserted%n", totalDocs);
            lastReportTime = currentTime;
          }
        }

        // Wait for refill to ensure we have data for next loop
        nextBatch.get();

        if (batchWaitMillis > 0) {
          Thread.sleep(batchWaitMillis);
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        totalErrors.add(1);
        // Log error but keep thread alive!
        System.err.printf("[Insert-Worker-%d] Error: %s%n", workerId, e.getMessage());

        // Optional: Short sleep on error to avoid hammering if DB is down
        try {
          Thread.sleep(1000);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private static class DocPrep {

    private final int docsPerIteration;
    private final boolean useStringIds;
    private final int workerId;
    private final List<String> payloads;
    private volatile List<Document> docs;
    private long sequenceNumber = 0;

    DocPrep(int docsPerIteration, boolean useStringIds, int workerId) {
      this.docsPerIteration = docsPerIteration;
      this.useStringIds = useStringIds;
      this.workerId = workerId;
      this.payloads = generatePayloads(100, PAYLOAD_REPEAT_COUNT);
      this.docs = new ArrayList<>();
    }

    private List<String> generatePayloads(int count, int size) {
      return IntStream.range(0, count)
          .mapToObj(i -> "foo".repeat(size))
          .collect(Collectors.toList());
    }

    public List<Document> getDocs() {
      return docs;
    }

    // Overload to use dedicated executor
    public CompletableFuture<Void> refill(ExecutorService executor) {
      return CompletableFuture.runAsync(() -> docs = fillDocs(), executor);
    }

    // Kept for backward compatibility if needed, though executor version is preferred
    public CompletableFuture<Void> refill() {
      return CompletableFuture.runAsync(() -> docs = fillDocs());
    }

    private BsonValue generateId() {
      if (useStringIds) {
        return new BsonString(
            workerId + "-" + sequenceNumber + "-" + RandomStringUtils.randomAlphanumeric(12)
        );
      }
      return new BsonObjectId(new ObjectId());
    }

    private List<Document> fillDocs() {
      List<Document> batch = new ArrayList<>(docsPerIteration);

      for (int i = 0; i < docsPerIteration; i++) {
        Document doc = new Document("_id", generateId())
            .append("seq", new BsonInt64(sequenceNumber++))
            .append("value", new BsonString(payloads.get(i % payloads.size())));
        batch.add(doc);
      }
      return batch;
    }
  }

  private static void validateConfig(
      String connectionString,
      int threadsCount,
      int durationMinutes,
      long batchWaitMillis,
      int docsPerIteration,
      String collectionName) {
    if (connectionString == null || connectionString.isBlank()) {
      throw new IllegalArgumentException("connectionString must not be blank");
    }
    if (threadsCount <= 0) {
      throw new IllegalArgumentException("threadsCount must be > 0");
    }
    if (durationMinutes <= 0) {
      throw new IllegalArgumentException("durationMinutes must be > 0");
    }
    if (batchWaitMillis < 0) {
      throw new IllegalArgumentException("batchWaitMillis must be >= 0");
    }
    if (docsPerIteration <= 0) {
      throw new IllegalArgumentException("docsPerIteration must be > 0");
    }
    if (collectionName == null || collectionName.isBlank()) {
      throw new IllegalArgumentException("collectionName must not be blank");
    }
  }
}