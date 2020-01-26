/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sources.v2.writer

import java.sql.{Connection, PreparedStatement}
import java.util.concurrent.atomic.AtomicLong
import java.util.{Locale, Optional}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{getCommonJDBCType, logWarning}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.dupdetect.NoOpDuplicateDetection
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class DuplicateAwareJdbcDataSourceV2 extends DuplicateAwareDataSourceV2 with DataSourceRegister with Logging {

  override val shortName = "djdbc"

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    val params = new JDBCOptions(options.asMap().toMap)
    val url = params.url
    val table = params.table
    val caseSensitive = options.getBoolean("spark.sql.caseSensitive", false)
    val dialect = JdbcDialects.get(url)
    val insertSql = JdbcUtils.getInsertStatement(table, schema, Option(schema), caseSensitive, dialect)

    val dupDetectClass = options.get(DuplicateAwareDataSourceV2.DUPLICATE_DETECTION).orElse(classOf[NoOpDuplicateDetection].getName)
    logInfo(s"Will use for duplicate detection: $dupDetectClass")
    val dupDetect: DuplicateDetection[Connection] = try {
      Class.forName(dupDetectClass).newInstance().asInstanceOf[DuplicateDetection[Connection]]
    } catch {
      case e: Exception => throw new RuntimeException(s"Could not load duplicate detection class $dupDetectClass", e)
    }

    logDebug(s"Creating a DuplicateAwareWriter for job $jobId")
    logDebug(s"Insert SQL: $insertSql")

    Optional.of(new JdbcWriter(jobId, params, dialect, insertSql, dupDetect))
  }
}

private class JdbcWriter(jobId: String, options: JDBCOptions, dialect: JdbcDialect, insertSql: String, dupDetect: DuplicateDetection[Connection]) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new JdbcDataWriterFactory(jobId, options, dialect, insertSql, dupDetect)
  }

  // we don't support 2PC (yet)
  override def commit(messages: Array[WriterCommitMessage]): Unit = { }

  override def abort(messages: Array[WriterCommitMessage]): Unit = { }
}

/** All writers created by this instance of the factory will run on the same executor. */
private class JdbcDataWriterFactory(jobId: String, options: JDBCOptions, dialect: JdbcDialect, insertSql: String, dupDetect: DuplicateDetection[Connection]) extends DataWriterFactory[Row] with Logging {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    logDebug(s"Creating a new DataWriter for partition $partitionId and job $jobId")
    new JdbcDataWriter(jobId, partitionId, options, dialect, insertSql, dupDetect)
  }

}

/** Created within the same executor, per partition. One writer will persist one partition. */
private class JdbcDataWriter(jobId: String, partitionId: Int, options: JDBCOptions, dialect: JdbcDialect, insertSql: String, duplicateDetection: DuplicateDetection[Connection]) extends DataWriter[Row] with Logging {

  import JdbcDataWriter._

  private lazy val (connection, supportsTransaction) = {
    logDebug(s"Preparing connection for partition $partitionId and job $jobId")
    val conn = JdbcUtils.createConnectionFactory(options)()

    val isolationLevel = getTransactionIsolationlevel(conn)
    val supportsTransactions = isolationLevel != Connection.TRANSACTION_NONE

    if (supportsTransactions) {
      conn.setAutoCommit(false) // Everything in the same db transaction.
      conn.setTransactionIsolation(isolationLevel)
    }

    (conn, supportsTransactions)
  }
  private lazy val stmt = {
    logDebug(s"Creating a prepared statement for: $insertSql")
    connection.prepareStatement(insertSql)
  }
  private val batchSize = options.batchSize
  private val isolationLevel = options.isolationLevel

  private def getTransactionIsolationlevel(conn: Connection): Int = {
    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }

    finalIsolationLevel
  }

  private val rowCounter = new AtomicLong(0L)

  /** Write single row to the database. Writing happens in batches, so this method invocation might not cause the
    * whole batch to be written. In case of transactions, persisting of the partition will only happen in the end. */
  override def write(record: Row): Unit = {
    val setters = record.schema.fields.map(f => makeSetter(connection, dialect, f.dataType))
    val nullTypes = record.schema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = record.schema.fields.length

    var i = 0
    while (i < numFields) {
      if (record.isNullAt(i)) {
        stmt.setNull(i + 1, nullTypes(i))
      } else {
        setters(i).apply(stmt, record, i)
      }
      i = i + 1
    }
    if (batchSize == 1) {
      stmt.executeUpdate()
    } else {
      if (rowCounter.incrementAndGet() % batchSize == 0) {
        stmt.executeBatch()
      } else {
        stmt.addBatch()
      }
    }

  }

  /** Persist the whole partition to the database. */
  override def commit(): WriterCommitMessage = {
    stmt.executeBatch()

    // commit partition id; if it already exists then this partition was previously persisted
    // and an exception will be thrown
    duplicateDetection.commitPartitionId(jobId, partitionId, connection)

    // commit partition
    if (supportsTransaction) {
      connection.commit()
    }

    try { stmt.close() } catch { case e: Exception => e.printStackTrace() }
    try { connection.close() } catch { case e: Exception => e.printStackTrace() }
    SuccessfulCommit(jobId, partitionId)
  }

  /** Rollback partition. */
  override def abort(): Unit = {
    if (supportsTransaction) {
      connection.rollback()
    }

    try { stmt.close() } catch { case e: Exception => e.printStackTrace() }
    try { connection.close() } catch { case e: Exception => e.printStackTrace() }
  }

}

object JdbcDataWriter {

  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
                          conn: Connection,
                          dialect: JdbcDialect,
                          dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase(Locale.ROOT).split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }
}

private case class SuccessfulCommit(jobId: String, partitionId: Int) extends WriterCommitMessage