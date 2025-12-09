package org.perf;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  public Runner(
      String connectionString,
      int threadsCount,
      int durationMinutes,
      long batchWaitMillis,
      int docsPerIteration,
      String collectionName,
      boolean useStringIds,
      boolean enableUpdatePhase) {

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
        .build();

    this.mongoClient = MongoClients.create(settings);

    // Use a dedicated pool for background data generation to avoid starving the commonPool
    this.backgroundGenPool = Executors.newCachedThreadPool();

    this.threads = new Thread[this.threadsCount];

    System.out.printf("Client connected to DB '%s'.%n", dbName);

    // Step 1: Cleanup existing search indexes and collection
    cleanupBeforeTest();

    // Step 2: Create collection and Atlas Search index
    createSearchIndex();

    // Step 3: Wait for index to be ready
    waitForIndexReady();

    // Phase 1: Insert phase
    System.out.println("\n=== Phase 1: INSERT ===");
    System.out.printf("Spawning %d threads...%n", threadsCount);
    Instant insertStartTime = Instant.now();
    Instant insertEndTime = insertStartTime.plus(Duration.ofMinutes(durationMinutes));

    for (int i = 0; i < this.threadsCount; i++) {
      final int workerId = i;
      threads[i] = new Thread(() -> doInsertWork(workerId, insertEndTime));
      threads[i].setName("Insert-Worker-" + i);
      threads[i].start();
    }
  }

  public void waitAndJoin() {
    try {
      // Wait for insert phase to complete
      for (Thread thread : threads) {
        if (thread != null) thread.join();
      }

      Instant insertPhaseEnd = Instant.now();
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
    } finally {
      printFinalSummary();
      cleanupAfterTest();
      shutdown();
    }
  }

  private void cleanupBeforeTest() {
    System.out.println("\nCleaning up existing search indexes and collections...");
    MongoDatabase db = mongoClient.getDatabase(dbName);

    // Drop existing search index
    try {
      db.getCollection(collectionName).dropSearchIndex(SEARCH_INDEX_NAME);
      System.out.printf("Dropped search index: %s%n", SEARCH_INDEX_NAME);
    } catch (Exception e) {
      System.out.printf("Search index %s does not exist or already dropped: %s%n",
          SEARCH_INDEX_NAME, e.getMessage());
    }

    // Drop collection
    try {
      db.getCollection(collectionName).drop();
      System.out.println("Dropped existing collection");
    } catch (Exception e) {
      System.out.printf("Collection does not exist: %s%n", e.getMessage());
    }
  }

  private void createSearchIndex() {
    System.out.println("\nCreating Atlas Search index...");
    MongoDatabase db = mongoClient.getDatabase(dbName);

    // Create collection
    db.createCollection(collectionName);
    System.out.printf("Created collection: %s%n", collectionName);

    // Create search index with dynamic mapping
    Document indexDefinition = new Document("mappings", new Document("dynamic", true));

    try {
      db.getCollection(collectionName).createSearchIndex(SEARCH_INDEX_NAME, indexDefinition);
      System.out.printf("Created Atlas Search index: %s%n", SEARCH_INDEX_NAME);
    } catch (Exception e) {
      System.err.println("Error creating search index: " + e.getMessage());
      System.err.println("Note: Atlas Search indexes are only available on MongoDB Atlas clusters");
    }
  }

  private void waitForIndexReady() {
    System.out.println("Waiting for index to be ready...");
    try {
      Thread.sleep(60000); // Wait 60 seconds for index to be created
      System.out.println("Index wait complete (60 seconds)");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Interrupted while waiting for index");
    }
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
    try {
      MongoDatabase db = mongoClient.getDatabase(dbName);
      System.out.println("Starting bulk update of all documents...");

      long startTime = System.currentTimeMillis();

      // Create update document with large payload similar to the JS script
      Document updateDoc = new Document("$set",
          new Document("value2", "bar".repeat(10 * 1024))
              .append("lastUpdated", new java.util.Date())
      );

      // Update all documents in the collection
      var result = db.getCollection(collectionName).updateMany(
          new Document(), // Empty filter = all documents
          updateDoc
      );

      long endTime = System.currentTimeMillis();
      double durationSeconds = (endTime - startTime) / 1000.0;

      totalUpdated.add(result.getModifiedCount());

      System.out.printf("Bulk update completed: %d documents updated in %.1f seconds (%.0f docs/sec)%n",
          result.getModifiedCount(), durationSeconds, result.getModifiedCount() / durationSeconds);

    } catch (Exception e) {
      totalErrors.add(1);
      System.err.println("Error during bulk update: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void printInsertPhaseSummary() {
    System.out.println("\n=== Insert Phase Complete ===");
    System.out.printf("Total Docs Inserted: %,d%n", totalInserted.sum());
    System.out.printf("Total Errors:        %,d%n", totalErrors.sum());

    double totalSeconds = durationMinutes * 60.0;
    double throughput = totalInserted.sum() / totalSeconds;
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
    long docsInsertedSinceLastReport = 0;

    while (Instant.now().isBefore(endTime)) {
      try {
        // Swap batches
        List<Document> batch = prep.getDocs();

        // Trigger next refill asynchronously on dedicated pool
        CompletableFuture<Void> nextBatch = prep.refill(backgroundGenPool);

        // Insert (The actual work)
        db.getCollection(collectionName).insertMany(batch);
        totalInserted.add(batch.size());
        docsInsertedSinceLastReport += batch.size();

        // Progress reporting every 10 seconds (only from worker 0 to avoid spam)
        if (workerId == 0) {
          long currentTime = System.currentTimeMillis();
          if (currentTime - lastReportTime >= 10000) {
            long totalDocs = totalInserted.sum();
            System.out.printf("Insert progress: %,d docs inserted%n", totalDocs);
            lastReportTime = currentTime;
            docsInsertedSinceLastReport = 0;
          }
        }

        // Wait for refill to ensure we have data for next loop
        nextBatch.get();

        if (batchWaitMillis > 0) {
          Thread.sleep(batchWaitMillis);
        }

      } catch (Exception e) {
        totalErrors.add(1);
        // Log error but keep thread alive!
        System.err.printf("[Insert-Worker-%d] Error: %s%n", workerId, e.getMessage());

        // Optional: Short sleep on error to avoid hammering if DB is down
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
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
      // Generate large payloads similar to JS script: "foo".repeat(10 * 1024) = 30KB
      this.payloads = generatePayloads(100, 10 * 1024);
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
        // FIXED: Include workerId to guarantee uniqueness across threads
        // Format: randomString - workerId - sequence
        return new BsonString(
            RandomStringUtils.randomAlphanumeric(9) + "-" + workerId + "-" + (sequenceNumber % 10000)
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
}