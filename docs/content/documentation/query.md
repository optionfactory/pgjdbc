---
title: "Issuing a Query and Processing the Result"
date: 2022-06-19T22:46:55+05:30
draft: false
weight: 4
toc: true
aliases:
    - "/documentation/head/query.html"
    - "/documentation/head/statement.html"
    - "/documentation/head/resultset.html"
    - "/documentation/head/update.html"
    - "/documentation/head/ddl.html"
    - "/documentation/head/java8-date-time.html"
    - "/documentation/80/query.html"
    - "/documentation/80/statement.html"
    - "/documentation/80/resultset.html"
    - "/documentation/80/update.html"
    - "/documentation/80/ddl.html"
    - "/documentation/81/query.html"
    - "/documentation/81/statement.html"
    - "/documentation/81/resultset.html"
    - "/documentation/81/update.html"
    - "/documentation/81/ddl.html"
    - "/documentation/82/query.html"
    - "/documentation/82/statement.html"
    - "/documentation/82/resultset.html"
    - "/documentation/82/update.html"
    - "/documentation/82/ddl.html"
    - "/documentation/83/query.html"
    - "/documentation/83/statement.html"
    - "/documentation/83/resultset.html"
    - "/documentation/83/update.html"
    - "/documentation/83/ddl.html"
    - "/documentation/84/query.html"
    - "/documentation/84/statement.html"
    - "/documentation/84/resultset.html"
    - "/documentation/84/update.html"
    - "/documentation/84/ddl.html"
    - "/documentation/85/query.html"
    - "/documentation/85/statement.html"
    - "/documentation/85/resultset.html"
    - "/documentation/85/update.html"
    - "/documentation/85/ddl.html"
    - "/documentation/90/query.html"
    - "/documentation/90/statement.html"
    - "/documentation/90/resultset.html"
    - "/documentation/90/update.html"
    - "/documentation/90/ddl.html"
    - "/documentation/91/query.html"
    - "/documentation/91/statement.html"
    - "/documentation/91/resultset.html"
    - "/documentation/91/update.html"
    - "/documentation/91/ddl.html"
    - "/documentation/92/query.html"
    - "/documentation/92/statement.html"
    - "/documentation/92/resultset.html"
    - "/documentation/92/update.html"
    - "/documentation/92/ddl.html"
    - "/documentation/93/query.html"
    - "/documentation/93/statement.html"
    - "/documentation/93/resultset.html"
    - "/documentation/93/update.html"
    - "/documentation/93/ddl.html"
    - "/documentation/94/query.html"
    - "/documentation/94/statement.html"
    - "/documentation/94/resultset.html"
    - "/documentation/94/update.html"
    - "/documentation/94/ddl.html"
---

Any time you want to issue SQL statements to the database, you require a `Statement` or `PreparedStatement` instance. Once you have a `Statement` or `PreparedStatement` , you can use issue a query. This will return a `ResultSet` instance, which contains the entire result (see the section called [Getting results based on a cursor](/documentation/query/#getting-results-based-on-a-cursor) here for how to alter this behaviour). [Example 5.1, “Processing a Simple Query in JDBC”](/documentation/query/#example51processing-a-simple-query-in-jdbc) illustrates this process.

##### Example 5.1. Processing a Simple Query in JDBC

This example will issue a simple query and print out the first column of each row using a `Statement` .

```java
Statement st = conn.createStatement();
ResultSet rs = st.executeQuery("SELECT * FROM mytable WHERE columnfoo = 500");
while (rs.next()) {
    System.out.print("Column 1 returned ");
    System.out.println(rs.getString(1));
}
rs.close();
st.close();
```

This example issues the same query as before but uses a `PreparedStatement` and a bind value in the query.

```java
int foovalue = 500;
PreparedStatement st = conn.prepareStatement("SELECT * FROM mytable WHERE columnfoo = ?");
st.setInt(1, foovalue);
ResultSet rs = st.executeQuery();
while (rs.next()) {
    System.out.print("Column 1 returned ");
    System.out.println(rs.getString(1));
}
rs.close();
st.close();
```

## Getting results based on a cursor

By default, the driver collects all the results for the query at once. This can be inconvenient for large data sets so 
the JDBC driver provides a means of basing a `ResultSet` on a database cursor and only fetching a small number of rows.

A small number of rows are cached on the client side of the connection and when exhausted the next block of rows is 
retrieved by repositioning the cursor.

> **NOTE**
>
> Cursor based `ResultSets` cannot be used in all situations. There a number of restrictions which will make the driver 
> silently fall back to fetching the whole `ResultSet` at once.
>
> * The connection to the server must be using the V3 protocol. This is the default for (and is only supported by) 
> server versions 7.4 and later.
>
> * The `Connection` must not be in autocommit mode. The backend closes cursors at the end of transactions, so in 
> autocommit mode the backend will have closed the cursor before anything can be fetched from it.
>
> * The `Statement` must be created with a `ResultSet` type of `ResultSet.TYPE_FORWARD_ONLY`. This is the default,
> so no code will need to be rewritten to take advantage of this, but it also means that you cannot scroll backwards or 
> otherwise jump around in the `ResultSet`.
>
> * The query given must be a single statement, not multiple statements strung together with semicolons.

##### Example 5.2. Setting fetch size to turn cursors on and off.

Changing the code to use cursor mode is as simple as setting the fetch size of the `Statement` to the appropriate size. 
Setting the fetch size back to 0 will cause all rows to be cached (the default behaviour).

```java
// make sure autocommit is off
conn.setAutoCommit(false);
Statement st = conn.createStatement();

// Turn use of the cursor on.
st.setFetchSize(50);
ResultSet rs = st.executeQuery("SELECT * FROM mytable");
while (rs.next()) {
    System.out.print("a row was returned.");
}
rs.close();

// Turn the cursor off.
st.setFetchSize(0);
rs = st.executeQuery("SELECT * FROM mytable");
while (rs.next()) {
    System.out.print("many rows were returned.");
}
rs.close();

// Close the statement.
st.close();
```

## Streaming results

With the `streamResults` connection property set to `true`, the driver reads the rows of a forward-only `ResultSet`
from the socket only when `next()` needs them. The query is sent once, as usual: the backend sends its rows as it
produces them, and stops while the driver does not read them. Unlike a cursor based `ResultSet`, this works in
autocommit mode and there is no round trip between batches of rows. The fetch size sets how many rows are read at a
time, 1024 when it is not set. A forward-only `ResultSet` is streamed even where the driver would otherwise use a
cursor, as described in the previous section.

An open connection can turn streaming on or off with `PGConnection.setStreamResults(boolean)`. The change applies to
the queries executed after it, by new and existing statements; result sets already open are not affected.

With the `streamResultsMaxWait` connection property set to a positive number of milliseconds, a batch does not wait
longer than that to fill up: when the time runs out, `next()` returns with the rows read so far, at least one, even if
they are fewer than the fetch size. The time counts from when the driver starts reading the batch, that is when the
query is sent or when `next()` needs the next batch. Rows that arrive quickly still fill whole batches, while a slow
query hands its rows over as they come, so one large fetch size suits fast and slow queries alike. PostgreSQL buffers
the rows it sends, about 8 kB at a time: the small rows of a slow query reach the driver when that buffer fills up or
the query ends. `streamResultsMaxWait` does not apply when the rest of a result is read in memory. An open
connection can change it with `PGConnection.setStreamResultsMaxWait(int)`: the change applies to the queries
executed after it, and a result set keeps the value its query started with.

Until the `ResultSet` has read its last row or is closed, the rest of its rows are still in the socket:

* Any other operation on the same connection, such as running another statement, committing, or a `COPY`, first
  reads the rest of the result in memory. The `ResultSet` stays valid.

* Closing the `ResultSet` before `next()` returns `false` sends a cancel request for its query and discards what the
  backend sent.
  The query is aborted, and so is the transaction if one is open: the following statements fail until it is rolled
  back, and `Connection.getWarnings()` reports the abort. A statement that has produced all its rows before the
  cancel request arrives, such as an `INSERT ... RETURNING`, completes anyway.

* `maxResultBuffer` limits each batch of rows. When the rest of a result is read in memory, it limits the rest as a
  whole: exceeding it closes the connection, as for a result that is not streamed.

* `Statement.setQueryTimeout` limits the time until the first batch of rows. PostgreSQL counts the time the
  application spends reading rows towards `statement_timeout`.

* `adaptiveFetch` does not apply to streamed results.

The following read the whole result, as before:

* scrollable result sets;
* multi-statement queries;
* statements run over the simple protocol (every statement with `preferQueryMode=simple`, and a plain `Statement` with
  `preferQueryMode=extendedForPrepared`);
* statements that return generated keys;
* statements protected by an `autosave` savepoint.

##### Streaming a large result

```java
// jdbc:postgresql://localhost/test?streamResults=true&streamResultsMaxWait=100
try (Statement st = conn.createStatement();
     ResultSet rs = st.executeQuery("SELECT * FROM mytable")) {
    while (rs.next()) {
        process(rs);
    }
}
```

## Using the Statement or PreparedStatement Interface

The following must be considered when using the `Statement` or `PreparedStatement` interface:

* You can use a single `Statement` instance as many times as you want. You could create one as soon as you open the 
connection and use it for the connection's lifetime. But you have to remember that only one `ResultSet` can exist 
per `Statement` or `PreparedStatement` at a given time.

* If you need to perform a query while processing a `ResultSet`, you can simply create and use another `Statement` .

* If you are using threads, and several are using the database, you must use a separate `Statement` for each thread. 
Refer to [Using the Driver in a Multithreaded or a Servlet Environment](/documentation/thread/) if you are thinking of 
using threads, as it covers some important points.

* When you are done using the `Statement` or `PreparedStatement` you should close it.

* In JDBC, the question mark (`?`) is the placeholder for the positional parameters of a `PreparedStatement`. 
There are, however, a number of PostgreSQL® operators that contain a question mark. To keep such question marks in an SQL 
statement from being interpreted as positional parameters, use two question marks ( `??` ) as escape sequence. 
You can also use this escape sequence in a `Statement` , but that is not required. Specifically only in a `Statement` 
a single ( `?` ) can be used as an operator.

## Using the ResultSet Interface

The following must be considered when using the `ResultSet` interface:

* Before reading any values, you must call `next()`. This returns true if there is a result, but more importantly, 
it prepares the row for processing.

* You must close a `ResultSet` by calling `close()` once you have finished using it.

* Once you make another query with the `Statement` used to create a `ResultSet`, the currently open `ResultSet` instance 
is closed automatically.

* When PreparedStatement API is used,  `ResultSet` switches to binary mode after five query executions (this default is 
set by the `prepareThreshold` connection property, see [Server Prepared Statements](/documentation/server-prepare/#server-prepared-statements). 
This may cause unexpected behaviour when some methods are called. For example, results on method calls such as `getString()` 
on non-string data types, while logically equivalent, may be formatted differently after execution exceeds the set 
`prepareThreshold` when conversion to object method switches to the method with a return type matching the return mode.

## Performing Updates

To change data (perform an `INSERT` , `UPDATE` , or `DELETE` ) you use the `executeUpdate()` method. This method is 
similar to the method `executeQuery()`

used to issue a `SELECT` statement, but it doesn't return a `ResultSet` instead it returns the number of rows affected 
by the `INSERT` , `UPDATE` , or `DELETE` statement. [Example 5.3, “Deleting Rows in JDBC”](/documentation/query/#example53deleting-rows-in-jdbc) 
illustrates the usage.

##### Example 5.3. Deleting Rows in JDBC

This example will issue a simple `DELETE` statement and print out the number of rows deleted.

```java
int foovalue = 500;
PreparedStatement st = conn.prepareStatement("DELETE FROM mytable WHERE columnfoo = ?");
st.setInt(1, foovalue);
int rowsDeleted = st.executeUpdate();
System.out.println(rowsDeleted + " rows deleted");
st.close();
```

## Creating and Modifying Database Objects

To create, modify or drop a database object like a table or view you use the `execute()` method.  This method is similar
to the method `executeQuery()` , but it doesn't return a result.
[Example 5.4, “Dropping a Table in JDBC](/documentation/query/#example54dropping-a-table-in-jdbc) illustrates the usage.

##### Example 5.4. Dropping a Table in JDBC

This example will drop a table.

```java
Statement st = conn.createStatement();
st.execute("DROP TABLE mytable");
st.close();
```

## Using Java 8 Date and Time classes

The PostgreSQL® JDBC driver implements native support for the [Java 8 Date and Time API](http://www.oracle.com/technetwork/articles/java/jf14-date-time-2125367.html)(JSR-310) using JDBC 4.2.

##### Table 5.1. Supported Java 8 Date and Time classes

|PostgreSQL®|Java SE 8|
|---|---|
|DATE|LocalDate|
|TIME [ WITHOUT TIME ZONE ]|LocalTime|
|TIMESTAMP [ WITHOUT TIME ZONE ]|LocalDateTime|
|TIMESTAMP WITH TIME ZONE|OffsetDateTime|

This is closely aligned with tables B-4 and B-5 of the JDBC 4.2 specification.

> **Note**
>
> `ZonedDateTime` , `Instant` and `OffsetTime / TIME WITH TIME ZONE` are not supported. Also note that all `OffsetDateTime` instances will have be in UTC (have offset 0). This is because the backend stores them as UTC.

**Example 5.2. Reading Java 8 Date and Time values using JDBC**

```java
Statement st = conn.createStatement();
ResultSet rs = st.executeQuery("SELECT * FROM mytable WHERE columnfoo = 500");
while (rs.next()) {
    System.out.print("Column 1 returned ");
    LocalDate localDate = rs.getObject(1, LocalDate.class);
    System.out.println(localDate);
}
rs.close();
st.close();
```

For other data types simply pass other classes to `#getObject` .

> **Note**
>
> The Java data types needs to match the SQL data types in table 7.1.

##### Example 5.3. Writing Java 8 Date and Time values using JDBC

```java
LocalDate localDate = LocalDate.now();
PreparedStatement st = conn.prepareStatement("INSERT INTO mytable (columnfoo) VALUES (?)");
st.setObject(1, localDate);
st.executeUpdate();
st.close();
```
