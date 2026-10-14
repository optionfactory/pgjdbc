/*
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.TransactionState;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.test.TestUtil;
import org.postgresql.util.PSQLState;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * Tests for {@code streamResults}: forward-only result sets read from the socket as they are
 * consumed, without a portal and without buffering the whole result.
 */
@ParameterizedClass
@MethodSource("data")
public class StreamResultsTest extends BaseTest4 {

  /** Large enough not to fit in the socket buffers. */
  private static final String LARGE = "SELECT i, repeat('x', 200) FROM generate_series(1, 1000000) i";

  /** Large enough not to fit in the socket buffers, small enough to be read in memory. */
  private static final String MEDIUM = "SELECT i, repeat('x', 200) FROM generate_series(1, 100000) i";

  public StreamResultsTest(BinaryMode binaryMode) {
    setBinaryMode(binaryMode);
    // results are streamed only over the extended protocol, whatever the mode the build tests
    setPreferQueryMode(PreferQueryMode.EXTENDED);
  }

  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<>();
    for (BinaryMode binaryMode : BinaryMode.values()) {
      ids.add(new Object[]{binaryMode});
    }
    return ids;
  }

  @BeforeAll
  static void createTables() throws Exception {
    try (Connection con = TestUtil.openDB()) {
      TestUtil.createTable(con, "stream_results", "id integer, payload text");
      TestUtil.execute(con, "CREATE OR REPLACE FUNCTION stream_results_insert(i integer) "
          + "RETURNS integer LANGUAGE sql VOLATILE AS "
          + "'INSERT INTO stream_results VALUES ($1, null) RETURNING id'");
    }
  }

  @AfterAll
  static void dropTables() throws Exception {
    try (Connection con = TestUtil.openDB()) {
      TestUtil.execute(con, "DROP FUNCTION IF EXISTS stream_results_insert(integer)");
      TestUtil.dropTable(con, "stream_results");
    }
  }

  @Override
  protected void updateProperties(Properties props) {
    super.updateProperties(props);
    PGProperty.STREAM_RESULTS.set(props, true);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    TestUtil.execute(con, "TRUNCATE stream_results");
  }

  @Override
  public void tearDown() throws SQLException {
    if (!con.getAutoCommit()) {
      con.rollback();
      con.setAutoCommit(true);
    }
    super.tearDown();
  }

  @Test
  void streamsAllRowsWithoutTransaction() throws Exception {
    assertTrue(con.getAutoCommit());
    assertEquals(1000000, countRows(LARGE, 0));
  }

  @Test
  void streamsAllRowsInTransaction() throws Exception {
    con.setAutoCommit(false);
    assertEquals(1000000, countRows(LARGE, 0));
    assertEquals(1000000, countRows(LARGE, 7));
    con.commit();
  }

  @Test
  void streamsSmallResults() throws Exception {
    assertEquals(0, countRows("SELECT 1 WHERE false", 0));
    assertEquals(1, countRows("SELECT 1", 0));
    assertEquals(10, countRows("SELECT generate_series(1, 10)", 1));
    assertEquals(10, countRows("SELECT generate_series(1, 10)", 10));
  }

  @Test
  void leavesTheRestOfTheResultToTheBackend() throws Exception {
    assumeMinimumServerVersion("wait_event ClientWrite requires PostgreSQL 10", ServerVersion.v10);
    int pid = ((PGConnection) con).getBackendPID();
    try (Statement stmt = con.createStatement();
         ResultSet rs = stmt.executeQuery(LARGE);
         Connection observer = TestUtil.openDB();
         PreparedStatement activity = observer.prepareStatement(
             "SELECT state, wait_event FROM pg_stat_activity WHERE pid = ?")) {
      assertTrue(rs.next());
      activity.setInt(1, pid);
      // the backend waits for the driver to read: the result is not buffered in the driver
      String waitEvent = null;
      for (int i = 0; i < 100 && !"ClientWrite".equals(waitEvent); i++) {
        try (ResultSet a = activity.executeQuery()) {
          assertTrue(a.next());
          assertEquals("active", a.getString(1));
          waitEvent = a.getString(2);
        }
        Thread.sleep(50);
      }
      assertEquals("ClientWrite", waitEvent);
    }
  }

  @Test
  void earlyCloseCancelsTheQueryWithoutTransaction() throws Exception {
    // each row inserts a row: the statement is rolled back with its implicit transaction
    try (Statement stmt = con.createStatement()) {
      stmt.setFetchSize(10);
      try (ResultSet rs = stmt.executeQuery(
          "SELECT stream_results_insert(i), repeat('x', 200) FROM generate_series(1, 1000000) i")) {
        assertTrue(rs.next());
      }
      assertEquals(0, count("SELECT count(*) FROM stream_results"));
      assertNotEquals(TransactionState.FAILED, ((PgConnection) con).getTransactionState());
      assertNull(con.getWarnings(), "no transaction was aborted");
    }
  }

  @Test
  void earlyCloseAbortsTheTransaction() throws Exception {
    con.setAutoCommit(false);
    TestUtil.execute(con, "INSERT INTO stream_results VALUES (1, 'before')");
    try (Statement stmt = con.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(LARGE)) {
        assertTrue(rs.next());
      }
      assertEquals(TransactionState.FAILED, ((PgConnection) con).getTransactionState());
      SQLWarning warning = con.getWarnings();
      assertNotNull(warning);
      assertEquals(PSQLState.QUERY_CANCELED.getState(), warning.getSQLState());

      SQLException e = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
      assertEquals(PSQLState.IN_FAILED_SQL_TRANSACTION.getState(), e.getSQLState());
      con.rollback();
    }
    assertEquals(0, count("SELECT count(*) FROM stream_results"));
  }

  @Test
  void setStreamResultsFalseStopsStreamingTheNextQueries() throws Exception {
    PGConnection pgConnection = con.unwrap(PGConnection.class);
    con.setAutoCommit(false);
    try (Statement stmt = con.createStatement()) {
      pgConnection.setStreamResults(false);
      assertFalse(pgConnection.getStreamResults());
      try (ResultSet rs = stmt.executeQuery(MEDIUM)) {
        assertTrue(rs.next());
      }
      // the whole result was read: closing it early cancels nothing
      assertEquals(TransactionState.OPEN, ((PgConnection) con).getTransactionState(),
          "a statement created before setStreamResults(false) streamed its next query");
    }
  }

  @Test
  void setStreamResultsTrueStreamsTheNextQueries() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    PGProperty.STREAM_RESULTS.set(props, false);
    try (Connection notStreaming = TestUtil.openDB(props);
         Statement stmt = notStreaming.createStatement()) {
      PGConnection pgConnection = notStreaming.unwrap(PGConnection.class);
      notStreaming.setAutoCommit(false);
      pgConnection.setStreamResults(true);
      assertTrue(pgConnection.getStreamResults());
      try (ResultSet rs = stmt.executeQuery(MEDIUM)) {
        assertTrue(rs.next());
      }
      // the result was streamed: closing it early cancels the query
      assertEquals(TransactionState.FAILED,
          notStreaming.unwrap(PgConnection.class).getTransactionState(),
          "a statement created before setStreamResults(true) did not stream its next query");
      notStreaming.rollback();
    }
  }

  @Test
  void closeAfterTheQueryEndedDoesNotCancelTheNextOne() throws Exception {
    // the backend sends the whole result and waits for a command: the cancel request lands
    // while it reads one, so PostgreSQL ignores it
    con.setAutoCommit(false);
    try (Statement stmt = con.createStatement()) {
      stmt.setFetchSize(10);
      for (int i = 0; i < 50; i++) {
        try (ResultSet rs = stmt.executeQuery("SELECT generate_series(1, 50)")) {
          assertTrue(rs.next());
          Thread.sleep(2);
        }
        assertEquals(1, count("SELECT 1"));
      }
    }
    assertNotEquals(TransactionState.FAILED, ((PgConnection) con).getTransactionState());
    con.commit();
  }

  @Test
  void closeAfterTheLastRowSendsNoCancel() throws Exception {
    con.setAutoCommit(false);
    assertEquals(1000000, countRows(LARGE, 100));
    assertEquals(TransactionState.OPEN, ((PgConnection) con).getTransactionState());
    con.commit();
  }

  @Test
  void anotherStatementReadsTheRestInMemory() throws Exception {
    try (Statement first = con.createStatement(); Statement second = con.createStatement()) {
      first.setFetchSize(100);
      try (ResultSet rs = first.executeQuery("SELECT generate_series(1, 10000)")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(42, count(second, "SELECT 42"));
        int last = 1;
        int rows = 1;
        while (rs.next()) {
          assertEquals(last + 1, rs.getInt(1));
          last = rs.getInt(1);
          rows++;
        }
        assertEquals(10000, rows);
      }
    }
  }

  @Test
  void commitReadsTheRestInMemory() throws Exception {
    con.setAutoCommit(false);
    try (PreparedStatement ps = con.prepareStatement("SELECT generate_series(1, ?)")) {
      ps.setFetchSize(50);
      ps.setInt(1, 1000);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        con.commit();
        int rows = 1;
        while (rs.next()) {
          rows++;
        }
        assertEquals(1000, rows);
      }
    }
  }

  @Test
  void maxResultBufferLimitsEachBatch() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    PGProperty.MAX_RESULT_BUFFER.set(props, "100000");
    try (Connection limited = TestUtil.openDB(props);
         Statement stmt = limited.createStatement()) {
      // 1000 rows of 200 bytes: two megabytes in all, 20 kilobytes per batch
      stmt.setFetchSize(100);
      assertEquals(1000, countRows(stmt.executeQuery(
          "SELECT repeat('x', 200) FROM generate_series(1, 1000)")));
    }
  }

  @Test
  void readingTheRestBeyondMaxResultBufferFailsTheNextStatement() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    PGProperty.MAX_RESULT_BUFFER.set(props, "100000");
    try (Connection limited = TestUtil.openDB(props);
         Statement stmt = limited.createStatement()) {
      stmt.setFetchSize(100);
      ResultSet rs = stmt.executeQuery("SELECT repeat('x', 200) FROM generate_series(1, 1000)");
      assertTrue(rs.next());
      // the rest of the result does not fit in maxResultBuffer: the connection is closed
      SQLException e = assertThrows(SQLException.class, () -> limited.createStatement().execute("SELECT 1"));
      assertEquals(PSQLState.COMMUNICATION_ERROR.getState(), e.getSQLState());
      assertTrue(limited.isClosed());
    }
  }

  @Test
  void errorsInTheMiddleOfTheResultAreThrownByNext() throws Exception {
    try (Statement stmt = con.createStatement()) {
      stmt.setFetchSize(100);
      int[] rows = {0};
      try (ResultSet rs = stmt.executeQuery("SELECT 1 / (5000 - i) FROM generate_series(1, 10000) i")) {
        SQLException e = assertThrows(SQLException.class, () -> {
          while (rs.next()) {
            rows[0]++;
          }
        });
        assertEquals(PSQLState.DIVISION_BY_ZERO.getState(), e.getSQLState());
      }
      // the rows before the error were returned
      assertTrue(rows[0] > 0 && rows[0] < 5000, () -> rows[0] + " rows");
      assertEquals(1, count(stmt, "SELECT 1"));
    }
  }

  @Test
  void maxRowsIsHonoured() throws Exception {
    try (Statement stmt = con.createStatement()) {
      stmt.setMaxRows(5);
      stmt.setFetchSize(2);
      int rows = 0;
      try (ResultSet rs = stmt.executeQuery("SELECT generate_series(1, 100)")) {
        while (rs.next()) {
          rows++;
        }
      }
      assertEquals(5, rows);
    }
  }

  @Test
  void scrollableResultSetsAreNotStreamed() throws Exception {
    try (Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      stmt.setFetchSize(2);
      try (ResultSet rs = stmt.executeQuery("SELECT generate_series(1, 100)")) {
        assertTrue(rs.last());
        assertEquals(100, rs.getInt(1));
        assertTrue(rs.first());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  void multipleStatementsAreNotStreamed() throws Exception {
    try (Statement stmt = con.createStatement()) {
      stmt.setFetchSize(1);
      assertTrue(stmt.execute("SELECT generate_series(1, 3); SELECT generate_series(1, 4)"));
      assertEquals(3, countRows(stmt.getResultSet()));
      assertTrue(stmt.getMoreResults());
      assertEquals(4, countRows(stmt.getResultSet()));
      assertFalse(stmt.getMoreResults());
    }
  }

  @Test
  void simpleQueryModeIsNotStreamed() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    PGProperty.PREFER_QUERY_MODE.set(props, PreferQueryMode.SIMPLE.value());
    try (Connection simple = TestUtil.openDB(props);
         Statement stmt = simple.createStatement()) {
      stmt.setFetchSize(1);
      assertTrue(stmt.execute("SELECT generate_series(1, 3); SELECT generate_series(1, 4)"));
      assertEquals(3, countRows(stmt.getResultSet()));
      assertTrue(stmt.getMoreResults());
      assertEquals(4, countRows(stmt.getResultSet()));
      assertFalse(stmt.getMoreResults());
    }
  }

  @Test
  void statementsProtectedByAutosaveAreNotStreamed() throws Exception {
    Properties props = new Properties();
    updateProperties(props);
    PGProperty.AUTOSAVE.set(props, AutoSave.ALWAYS.value());
    try (Connection autosave = TestUtil.openDB(props)) {
      autosave.setAutoCommit(false);
      try (Statement stmt = autosave.createStatement()) {
        stmt.setFetchSize(10);
        try (ResultSet rs = stmt.executeQuery("SELECT generate_series(1, 1000)")) {
          assertTrue(rs.next());
        }
        assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1 / 0"));
        // autosave rolled back to its savepoint: the transaction is usable
        assertEquals(1000, countRows(stmt.executeQuery("SELECT generate_series(1, 1000)")));
        autosave.commit();
      }
    }
  }

  @Test
  void statementsReturningGeneratedKeysAreNotStreamed() throws Exception {
    con.setAutoCommit(false);
    try (Statement stmt = con.createStatement()) {
      // the update count comes after the last row: the keys are read whole before it
      assertEquals(100000, stmt.executeUpdate(
          "INSERT INTO stream_results SELECT i, repeat('x', 200) FROM generate_series(1, 100000) i",
          Statement.RETURN_GENERATED_KEYS));
      try (ResultSet keys = stmt.getGeneratedKeys()) {
        assertTrue(keys.next());
      }
      // closing the keys early cancels nothing
      assertEquals(TransactionState.OPEN, ((PgConnection) con).getTransactionState(),
          "the generated keys were streamed");
    }
  }

  private int countRows(String sql, int fetchSize) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      stmt.setFetchSize(fetchSize);
      return countRows(stmt.executeQuery(sql));
    }
  }

  private static int countRows(ResultSet rs) throws SQLException {
    try (ResultSet ignored = rs) {
      int rows = 0;
      while (rs.next()) {
        rows++;
      }
      return rows;
    }
  }

  private int count(String sql) throws SQLException {
    try (Statement stmt = con.createStatement()) {
      return count(stmt, sql);
    }
  }

  private static int count(Statement stmt, String sql) throws SQLException {
    try (ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }
}
