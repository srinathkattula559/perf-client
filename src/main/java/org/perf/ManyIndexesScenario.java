package org.perf;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Updates;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

public class ManyIndexesScenario {

  private static final int DEFAULT_TOTAL_COLLECTIONS = 100;
  private static final int DEFAULT_NUM_ACTIVE_COLLECTIONS = 2;
  private static final int DEFAULT_DURATION_MINUTES = 10;
  private static final int DEFAULT_THREADS = 10;
  private static final int DEFAULT_DOCS_PER_BATCH = 5_000;
  private static final String BASE_COLL_NAME = "scale_test_";
  private static final String DEFAULT_DB_NAME = "scale_db";
  private static final String SEARCH_INDEX_NAME = "default";
  private static final Duration INDEX_READY_TIMEOUT = Duration.ofMinutes(20);
  private static final Duration INDEX_POLL_INTERVAL = Duration.ofSeconds(15);

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println(
          "Usage: java -cp target/perf-client-1.0-SNAPSHOT.jar "
              + "org.perf.ManyIndexesScenario <connection_string>");
      System.out.println(
          "Optional -D settings: totalCollections, activeCollections, durationMinutes, "
              + "threads, docsPerBatch");
      System.exit(1);
    }

    String connectionString = args[0];
    new ManyIndexesScenario(connectionString).run();
  }

  private final MongoClient client;
  private final MongoDatabase db;
  private final List<String> activeCollectionNames;
  private final int totalCollections;
  private final int numActiveCollections;
  private final int durationMinutes;
  private final int threads;
  private final int docsPerBatch;
  private final boolean cleanupAfterRun;

  public ManyIndexesScenario(String connectionString) {
    this.totalCollections = positiveProperty("totalCollections", DEFAULT_TOTAL_COLLECTIONS);
    this.numActiveCollections = positiveProperty("activeCollections", DEFAULT_NUM_ACTIVE_COLLECTIONS);
    this.durationMinutes = positiveProperty("durationMinutes", DEFAULT_DURATION_MINUTES);
    this.threads = positiveProperty("threads", DEFAULT_THREADS);
    this.docsPerBatch = positiveProperty("docsPerBatch", DEFAULT_DOCS_PER_BATCH);
    this.cleanupAfterRun = booleanProperty("cleanupAfterRun", true);
    if (numActiveCollections > totalCollections) {
      throw new IllegalArgumentException("activeCollections must be <= totalCollections");
    }

    ConnectionString parsedConnectionString = new ConnectionString(connectionString);
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(parsedConnectionString)
        .applyToSocketSettings(builder -> builder
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.MILLISECONDS))
        .applyToConnectionPoolSettings(builder -> builder
            .maxSize(Math.max(100, threads * 2)))
        .build();

    this.client = MongoClients.create(settings);
    this.db = client.getDatabase(
        parsedConnectionString.getDatabase() != null
            ? parsedConnectionString.getDatabase()
            : DEFAULT_DB_NAME);

    this.activeCollectionNames = new ArrayList<>();
    // Pick active collections evenly spaced out
    int step = Math.max(1, totalCollections / numActiveCollections);
    for (int i = 0; i < numActiveCollections; i++) {
      this.activeCollectionNames.add(BASE_COLL_NAME + (i * step));
    }
  }

  public void run() {
    System.out.println("=== Starting Basic Scale Test (No Oplog Desert) ===");
    System.out.printf(
        "Database: %s | Total Indexes: %d | Active Collection Count: %d%n",
        db.getName(), totalCollections, numActiveCollections);
    System.out.println("Active Collections List: " + activeCollectionNames);

    try {
      // 1. Setup Phase
      setupEnvironment();

      // 2. Wait until search indexes are queryable
      waitForSearchIndexesReady();

      // 3. Execution Phase
      runWorkload();
    } finally {
      if (cleanupAfterRun) {
        cleanupEnvironment();
      }
      client.close();
    }
  }

  private void setupEnvironment() {
    dropAllSearchIndexesInDatabase();
    dropAllCollectionsInDatabase();

    System.out.println("\n>>> Setting up " + totalCollections + " collections...");
    ExecutorService setupPool = Executors.newFixedThreadPool(50);
    List<Future<?>> setupTasks = new ArrayList<>();
    long start = System.currentTimeMillis();

    for (int i = 0; i < totalCollections; i++) {
      final int indexId = i;
      setupTasks.add(setupPool.submit(() -> {
        String collName = BASE_COLL_NAME + indexId;
        ensureCollection(collName);
        db.getCollection(collName).createIndex(Indexes.compoundIndex(
            Indexes.ascending("status"),
            Indexes.ascending("batchId")));
        ensureSearchIndex(collName);

        if (indexId % 20 == 0) {
          System.out.printf("Setup progress: %d/%d...%n", indexId, totalCollections);
        }
      }));
    }

    setupPool.shutdown();
    try {
      if (!setupPool.awaitTermination(45, TimeUnit.MINUTES)) {
        setupPool.shutdownNow();
        throw new IllegalStateException("Setup timed out");
      }
      for (Future<?> task : setupTasks) {
        task.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while setting up collections", e);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set up collections and indexes", e);
    }

    long duration = (System.currentTimeMillis() - start) / 1000;
    System.out.printf("Setup complete in %d seconds.%n", duration);
  }

  private void runWorkload() {
    System.out.println("\n>>> Starting Workload (Targeted Updates Only)...");

    ExecutorService workers = Executors.newFixedThreadPool(threads);
    AtomicLong totalOps = new AtomicLong(0);
    Instant end = Instant.now().plus(Duration.ofMinutes(durationMinutes));

    for (int t = 0; t < threads; t++) {
      workers.submit(() -> {
        while (Instant.now().isBefore(end)) {
          try {
            // Pick active collection
            String targetCollName = activeCollectionNames.get(
                ThreadLocalRandom.current().nextInt(activeCollectionNames.size())
            );

            // 1. Insert Batch (Status: new)
            String batchId = new ObjectId().toHexString();
            List<Document> batch = new ArrayList<>(docsPerBatch);
            for (int i = 0; i < docsPerBatch; i++) {
              batch.add(new Document("text", RandomStringUtils.randomAlphanumeric(50))
                  .append("ts", System.currentTimeMillis())
                  .append("batchId", batchId)
                  .append("status", "new"));
            }
            db.getCollection(targetCollName).insertMany(batch, new InsertManyOptions().ordered(false));

            // 2. TARGETED Update (No Desert). Batch ID isolates concurrent workers.
            var updateResult = db.getCollection(targetCollName).updateMany(
                Filters.and(Filters.eq("status", "new"), Filters.eq("batchId", batchId)),
                Updates.set("status", "processed")
            );

            totalOps.addAndGet(docsPerBatch + updateResult.getModifiedCount());

            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          } catch (Exception e) {
            System.err.println("Error in worker: " + e.getMessage());
          }
        }
      });
    }

    // Monitor loop
    try {
      long lastCount = 0;
      while (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
        if (Instant.now().isAfter(end)) {
          workers.shutdown();
        }
        long current = totalOps.get();
        System.out.printf("[Status] Total Ops: %,d (Rate: %,d ops/10s)%n",
            current, (current - lastCount));
        lastCount = current;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      workers.shutdownNow();
      throw new IllegalStateException("Interrupted while running workload", e);
    } finally {
      workers.shutdownNow();
    }
  }

  private void cleanupEnvironment() {
    System.out.println("\n>>> Cleaning up all collections in database...");
    dropAllCollectionsInDatabase();
  }

  private void ensureCollection(String collName) {
    for (String existingName : db.listCollectionNames()) {
      if (collName.equals(existingName)) {
        return;
      }
    }
    db.createCollection(collName);
  }

  private void ensureSearchIndex(String collName) {
    MongoCollection<Document> collection = db.getCollection(collName);
    Document definition = new Document("mappings", new Document("dynamic", true));
    collection.createSearchIndex(SEARCH_INDEX_NAME, definition);
  }

  private void dropAllSearchIndexesInDatabase() {
    System.out.println("\n>>> Dropping all existing search indexes in database...");
    int droppedCount = 0;
    for (String collName : db.listCollectionNames()) {
      MongoCollection<Document> collection = db.getCollection(collName);
      List<String> indexNames = new ArrayList<>();
      for (Document index : collection.listSearchIndexes()) {
        String name = index.getString("name");
        if (name != null && !name.isBlank()) {
          indexNames.add(name);
        }
      }

      for (String indexName : indexNames) {
        collection.dropSearchIndex(indexName);
        droppedCount++;
        System.out.printf("Dropped search index: %s.%s%n", collName, indexName);
      }
    }

    if (droppedCount == 0) {
      System.out.println("No existing search indexes found in database");
    }
  }

  private void dropAllCollectionsInDatabase() {
    List<String> collectionNames = new ArrayList<>();
    for (String collName : db.listCollectionNames()) {
      if (!collName.startsWith("system.")) {
        collectionNames.add(collName);
      }
    }

    if (collectionNames.isEmpty()) {
      System.out.println("No existing collections found in database");
      return;
    }

    for (String collName : collectionNames) {
      db.getCollection(collName).drop();
      System.out.printf("Dropped collection: %s%n", collName);
    }
  }

  private void waitForSearchIndexesReady() {
    System.out.println("\nWaiting for search indexes to become ready...");
    Instant deadline = Instant.now().plus(INDEX_READY_TIMEOUT);

    while (Instant.now().isBefore(deadline)) {
      int readyCount = 0;
      for (int i = 0; i < totalCollections; i++) {
        if (isSearchIndexReady(BASE_COLL_NAME + i)) {
          readyCount++;
        }
      }

      System.out.printf("Search index readiness: %d/%d%n", readyCount, totalCollections);
      if (readyCount == totalCollections) {
        return;
      }

      try {
        Thread.sleep(INDEX_POLL_INTERVAL.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for search indexes", e);
      }
    }

    throw new IllegalStateException("Timed out waiting for search indexes to become ready");
  }

  private boolean isSearchIndexReady(String collName) {
    MongoCollection<Document> collection = db.getCollection(collName);
    for (Document index : collection.listSearchIndexes()) {
      if (!SEARCH_INDEX_NAME.equals(index.getString("name"))) {
        continue;
      }
      String status = index.getString("status");
      Object queryable = index.get("queryable");
      return "READY".equalsIgnoreCase(status) || Boolean.TRUE.equals(queryable);
    }
    return false;
  }

  private static int positiveProperty(String name, int defaultValue) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed <= 0) {
        throw new IllegalArgumentException(name + " must be > 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be an integer", e);
    }
  }

  private static boolean booleanProperty(String name, boolean defaultValue) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new IllegalArgumentException(name + " must be true or false");
  }
}