package org.perf;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RunnerTest {

  @Test
  void constructorRejectsInvalidWorkloadShape() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Runner("mongodb://localhost:27017/testdb", 0, 5, 0, 100, "testColl", false, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Runner("mongodb://localhost:27017/testdb", 10, 0, 0, 100, "testColl", false, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Runner("mongodb://localhost:27017/testdb", 10, 5, -1, 100, "testColl", false, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Runner("mongodb://localhost:27017/testdb", 10, 5, 0, 0, "testColl", false, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Runner("mongodb://localhost:27017/testdb", 10, 5, 0, 100, " ", false, true));
  }

  @Test
  void constructorAcceptsValidConfigWithoutConnecting() {
    assertDoesNotThrow(
        () -> new Runner("mongodb://localhost:27017/testdb", 10, 5, 0, 100, "testColl", false, true));
  }
}
