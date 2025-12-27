package org.perf;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;

public class ManyIndexesScenario {

  // --- CONFIGURATION ---
  // Scaled to 1000 to make the "Idle vs Active" contrast obvious
  private static final int TOTAL_COLLECTIONS = 1000;
  private static final int NUM_ACTIVE_COLLECTIONS = 2;
  private static final String BASE_COLL_NAME = "scale_test_";
  private static final String DB_NAME = "scale_db";
  private static final String SEARCH_INDEX_NAME = "default";

  // Test Duration
  private static final int DURATION_MINUTES = 10;
  private static final int THREADS = 10;
  private static final int DOCS_PER_BATCH = 50;

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Usage: java -cp target/perf-client-1.0-SNAPSHOT.jar org.perf.ManyIndexesScenario <connection_string>");
      System.exit(1);
    }

    String connectionString = args[0];
    new ManyIndexesScenario(connectionString).run();
  }

  private final MongoClient client;
  private final MongoDatabase db;
  private final List<String> activeCollectionNames;

  public ManyIndexesScenario(String connectionString) {
    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(connectionString))
        .applyToSocketSettings(builder -> builder
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS))
        .build();

    this.client = MongoClients.create(settings);
    this.db = client.getDatabase(DB_NAME);

    this.activeCollectionNames = new ArrayList<>();
    // Pick active collections evenly spaced out
    int step = TOTAL_COLLECTIONS / NUM_ACTIVE_COLLECTIONS;
    for (int i = 0; i < NUM_ACTIVE_COLLECTIONS; i++) {
      this.activeCollectionNames.add(BASE_COLL_NAME + (i * step));
    }
  }

  public void run() {
    System.out.println("=== Starting Basic Scale Test (No Oplog Desert) ===");
    System.out.printf("Total Indexes: %d | Active Collection Count: %d%n", TOTAL_COLLECTIONS, NUM_ACTIVE_COLLECTIONS);
    System.out.println("Active Collections List: " + activeCollectionNames);

    // 1. Setup Phase
    setupEnvironment();

    // 2. Cooldown
    System.out.println("\nWaiting 60 seconds for indexes to stabilize...");
    sleep(60);

    // 3. Execution Phase
    runWorkload();

    client.close();
  }

  private void setupEnvironment() {
    System.out.println("\n>>> Setting up " + TOTAL_COLLECTIONS + " collections...");
    ExecutorService setupPool = Executors.newFixedThreadPool(50);
    long start = System.currentTimeMillis();

    for (int i = 0; i < TOTAL_COLLECTIONS; i++) {
      final int indexId = i;
      setupPool.submit(() -> {
        String collName = BASE_COLL_NAME + indexId;
        try {
          // Create Collection (ignore if exists)
          try { db.createCollection(collName); } catch (Exception ignored) {}

          // Create Search Index
          Document definition = new Document("mappings", new Document("dynamic", true));
          try {
            db.getCollection(collName).createSearchIndex(SEARCH_INDEX_NAME, definition);
          } catch (Exception ignored) {}

          if (indexId % 200 == 0) {
            System.out.printf("Setup progress: %d/%d...%n", indexId, TOTAL_COLLECTIONS);
          }
        } catch (Exception e) {
          System.err.printf("Failed to setup %s: %s%n", collName, e.getMessage());
        }
      });
    }

    setupPool.shutdown();
    try {
      if (!setupPool.awaitTermination(45, TimeUnit.MINUTES)) {
        System.err.println("Setup timed out! proceeding anyway...");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    long duration = (System.currentTimeMillis() - start) / 1000;
    System.out.printf("Setup complete in %d seconds.%n", duration);
  }

  private void runWorkload() {
    System.out.println("\n>>> Starting Workload (Targeted Updates Only)...");

    ExecutorService workers = Executors.newFixedThreadPool(THREADS);
    AtomicLong totalOps = new AtomicLong(0);
    Instant end = Instant.now().plus(Duration.ofMinutes(DURATION_MINUTES));

    for (int t = 0; t < THREADS; t++) {
      workers.submit(() -> {
        while (Instant.now().isBefore(end)) {
          try {
            // Pick active collection
            String targetCollName = activeCollectionNames.get(
                ThreadLocalRandom.current().nextInt(activeCollectionNames.size())
            );

            // 1. Insert Batch (Status: new)
            List<Document> batch = new ArrayList<>(DOCS_PER_BATCH);
            for (int i = 0; i < DOCS_PER_BATCH; i++) {
              batch.add(new Document("text", RandomStringUtils.randomAlphanumeric(50))
                  .append("ts", System.currentTimeMillis())
                  .append("status", "new"));
            }
            db.getCollection(targetCollName).insertMany(batch);

            // 2. TARGETED Update (No Desert)
            // Only update the documents we just inserted (status="new")
            // This keeps the oplog footprint small and proportional to the insert rate.
            db.getCollection(targetCollName).updateMany(
                Filters.eq("status", "new"),
                Updates.set("status", "processed")
            );

            totalOps.addAndGet(DOCS_PER_BATCH * 2L);

            Thread.sleep(50);
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
      e.printStackTrace();
    }
  }

  private void sleep(int seconds) {
    try { Thread.sleep(seconds * 1000L); } catch (InterruptedException e) {}
  }
}