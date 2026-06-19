package org.perf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MainTest {

  @Test
  void parseArgsAcceptsDocumentedCliShape() {
    Main.RunnerConfig config =
        Main.parseArgs(new String[] {
          "mongodb://localhost:27017/testdb",
          "10",
          "5",
          "0",
          "100",
          "testColl",
          "false",
          "true"
        });

    assertEquals("mongodb://localhost:27017/testdb", config.connectionString());
    assertEquals(10, config.threads());
    assertEquals(5, config.durationMinutes());
    assertEquals(0, config.batchWaitMillis());
    assertEquals(100, config.docsPerIteration());
    assertEquals("testColl", config.collectionName());
    assertEquals(false, config.useStringIds());
    assertEquals(true, config.enableUpdatePhase());
  }

  @Test
  void parseArgsRejectsWrongArgumentCount() {
    assertThrows(IllegalArgumentException.class, () -> Main.parseArgs(new String[] {}));
  }

  @Test
  void parseArgsRejectsNonPositiveDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Main.parseArgs(new String[] {
              "mongodb://localhost:27017/testdb",
              "10",
              "0",
              "0",
              "100",
              "testColl",
              "false",
              "true"
            }));
  }

  @Test
  void parseArgsRejectsInvalidBooleans() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Main.parseArgs(new String[] {
              "mongodb://localhost:27017/testdb",
              "10",
              "5",
              "0",
              "100",
              "testColl",
              "nope",
              "true"
            }));
  }

  @Test
  void runReturnsFailureForUsageErrors() {
    assertEquals(1, Main.run(new String[] {}));
  }
}
