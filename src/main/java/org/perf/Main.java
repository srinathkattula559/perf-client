package org.perf;

public class Main {
    public static void main(String[] args) {
        if (args.length < 8) {
            printUsage();
            System.exit(1);
        }
        try {
            String connectionString = args[0];
            int threadsCount = Integer.parseInt(args[1]);
            int durationMinutes = Integer.parseInt(args[2]);
            long batchWaitMillis = Long.parseLong(args[3]);
            int docsPerIteration = Integer.parseInt(args[4]);
            String collectionName = args[5];
            boolean useStringIds = Boolean.parseBoolean(args[6]);
            boolean enableUpdatePhase = Boolean.parseBoolean(args[7]);

            String maskedConnString = connectionString.length() > 20
                    ? connectionString.substring(0, 15) + "..."
                    : connectionString;

            System.out.printf(
                    "Starting Perf Client:\n" +
                            " - Connection string: %s\n" +
                            " - Threads count: %d\n" +
                            " - Duration: %d minutes\n" +
                            " - Batch wait: %d millis\n" +
                            " - Docs per iteration: %d\n" +
                            " - Collection name: %s\n" +
                            " - Use random string IDs: %b\n" +
                            " - Enable update phase: %b\n",
                    maskedConnString,
                    threadsCount,
                    durationMinutes,
                    batchWaitMillis,
                    docsPerIteration,
                    collectionName,
                    useStringIds,
                    enableUpdatePhase);

            final var runner = new Runner(
                    connectionString,
                    threadsCount,
                    durationMinutes,
                    batchWaitMillis,
                    docsPerIteration,
                    collectionName,
                    useStringIds,
                    enableUpdatePhase
            );

            runner.start();
            runner.waitAndJoin();

        } catch (NumberFormatException e) {
            System.err.println("Error parsing arguments. Ensure numeric values are correct.");
            System.err.println("Details: " + e.getMessage());
        } catch (Exception ex) {
            System.err.printf("Unexpected Exception: %s%n", ex.getMessage());
            ex.printStackTrace();
        }
        System.out.println("Perf Client - Done");
    }

    private static void printUsage() {
        System.out.println("Usage: java org.perf.Main <connStr> <threads> <duration> <batchWait> <docsPerIter> <collection> <useStringIds> <enableUpdatePhase>");
        System.out.println("Example: java org.perf.Main mongodb://localhost:27017 10 5 100 50 testColl true true");
        System.out.println("\nParameters:");
        System.out.println("  connStr           - MongoDB connection string");
        System.out.println("  threads           - Number of concurrent worker threads");
        System.out.println("  duration          - Insert phase duration in minutes");
        System.out.println("  batchWait         - Wait time between batches in milliseconds");
        System.out.println("  docsPerIter       - Number of documents per iteration");
        System.out.println("  collection        - Collection name");
        System.out.println("  useStringIds      - Use random string IDs (true/false)");
        System.out.println("  enableUpdatePhase - Run bulk update after inserts (true/false)");
    }
}
