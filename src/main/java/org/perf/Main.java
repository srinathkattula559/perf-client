package org.perf;

public class Main {
  private static final int EXPECTED_ARG_COUNT = 8;

  public static void main(String[] args) {
    int exitCode = run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args) {
    RunnerConfig config;
    try {
      config = parseArgs(args);
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid arguments: " + e.getMessage());
      printUsage();
      return 1;
    }

    try {
      System.out.printf(
          "Starting Perf Client:%n"
              + " - Connection string: %s%n"
              + " - Threads: %d%n"
              + " - Duration: %d minutes%n"
              + " - Batch wait: %d ms%n"
              + " - Docs per iteration: %d%n"
              + " - Collection: %s%n"
              + " - Use string IDs: %s%n"
              + " - Enable update phase: %s%n",
          maskConnectionString(config.connectionString()),
          config.threads(),
          config.durationMinutes(),
          config.batchWaitMillis(),
          config.docsPerIteration(),
          config.collectionName(),
          config.useStringIds(),
          config.enableUpdatePhase());

      Runner runner =
          new Runner(
              config.connectionString(),
              config.threads(),
              config.durationMinutes(),
              config.batchWaitMillis(),
              config.docsPerIteration(),
              config.collectionName(),
              config.useStringIds(),
              config.enableUpdatePhase());

      runner.start();
      runner.waitAndJoin();
      System.out.println("Perf Client - Done");
      return 0;

    } catch (Exception ex) {
      System.err.printf("Unexpected exception: %s%n", ex.getMessage());
      ex.printStackTrace();
      return 1;
    }
  }

  static RunnerConfig parseArgs(String[] args) {
    if (args.length != EXPECTED_ARG_COUNT) {
      throw new IllegalArgumentException(
          "expected " + EXPECTED_ARG_COUNT + " arguments but received " + args.length);
    }

    return new RunnerConfig(
        parseConnectionString(args[0]),
        parsePositiveInt(args[1], "threads"),
        parsePositiveInt(args[2], "duration"),
        parseNonNegativeLong(args[3], "batchWait"),
        parsePositiveInt(args[4], "docsPerIter"),
        parseCollectionName(args[5]),
        parseBoolean(args[6], "useStringIds"),
        parseBoolean(args[7], "enableUpdatePhase"));
  }

  private static int parsePositiveInt(String value, String name) {
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

  private static String parseConnectionString(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("connStr must not be blank");
    }
    return value;
  }

  private static long parseNonNegativeLong(String value, String name) {
    try {
      long parsed = Long.parseLong(value);
      if (parsed < 0) {
        throw new IllegalArgumentException(name + " must be >= 0");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be a number", e);
    }
  }

  private static boolean parseBoolean(String value, String name) {
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new IllegalArgumentException(name + " must be true or false");
  }

  private static String parseCollectionName(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("collection must not be blank");
    }
    return value;
  }

  private static String maskConnectionString(String connectionString) {
    return connectionString.replaceFirst("(?<=://)([^/@]+@)", "***@");
  }

  private static void printUsage() {
    System.err.println(
        "Usage: java -jar target/perf-client-1.0-SNAPSHOT.jar "
            + "<connStr> <threads> <duration> <batchWait> <docsPerIter> "
            + "<collection> <useStringIds> <enableUpdatePhase>");
    System.err.println(
        "Example: java -jar target/perf-client-1.0-SNAPSHOT.jar "
            + "\"mongodb+srv://user:pass@cluster.mongodb.net/testdb\" "
            + "10 5 0 100 testColl false true");
  }

  record RunnerConfig(
      String connectionString,
      int threads,
      int durationMinutes,
      long batchWaitMillis,
      int docsPerIteration,
      String collectionName,
      boolean useStringIds,
      boolean enableUpdatePhase) {}
}
