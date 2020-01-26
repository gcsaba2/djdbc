package org.apache.spark.sql.sources.v2.writer
import java.sql.{Connection, PreparedStatement}
import java.util.{Locale, Optional}

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import scala.collection.JavaConversions._

class DuplicateAwareJdbcDataSourceV2 extends DuplicateAwareDataSourceV2 with DataSourceRegister {

  override val shortName = "djdbc"

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    val params = new JDBCOptions(options.asMap().toMap)
    val url = params.url
    val table = params.table
    val caseSensitive = options.getBoolean("spark.sql.caseSensitive", false)
    val dialect = JdbcDialects.get(url)
    val insertSql = JdbcUtils.getInsertStatement("table", schema, Option(schema), caseSensitive, dialect)

    Optional.of(new JdbcWriter(jobId, params, dialect, insertSql))
  }
}

private class JdbcWriter(jobId: String, options: JDBCOptions, dialect: JdbcDialect, insertSql: String) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new JdbcDataWriterFactory(jobId, options, dialect, insertSql)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

private class JdbcDataWriterFactory(jobId: String, options: JDBCOptions, dialect: JdbcDialect, insertSql: String) extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new JdbcDataWriter(jobId, partitionId, options, dialect, insertSql)
  }

}

private class JdbcDataWriter(jobId: String, partitionId: Int, options: JDBCOptions, dialect: JdbcDialect, insertSql: String) extends DataWriter[Row] {

  import JdbcDataWriter._

  private lazy val connection = {
    val conn = JdbcUtils.createConnectionFactory(options)()
    conn.setAutoCommit(false)
    conn
  }
  /* TODO optimize pstmt creation
  private lazy val stmt = {
    val setters = record.schema.fields.map(f => makeSetter(connection, dialect, f.dataType))
    val nullTypes = record.schema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = record.schema.fields.length

    val stmt = connection.prepareStatement(insertSql)
  }
   */

  override def write(record: Row): Unit = {
    //JdbcUtils.savePartition(() => connection,
      //"n/a", Seq(record).iterator, record.schema, insertSql, 1, dialect, options.isolationLevel)
    /*
    rddSchema: StructType,
      insertStmt: String,
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int
     */
    val setters = record.schema.fields.map(f => makeSetter(connection, dialect, f.dataType))
    val nullTypes = record.schema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
    val numFields = record.schema.fields.length

    val stmt = connection.prepareStatement(insertSql)
    try {
        var i = 0
        while (i < numFields) {
          if (record.isNullAt(i)) {
            stmt.setNull(i + 1, nullTypes(i))
          } else {
            setters(i).apply(stmt, record, i)
          }
          i = i + 1
        }
        stmt.executeUpdate()
    } finally {
      stmt.close()
    }
    //if (supportsTransactions) {
      //connection.commit()
    //}
    //committed = true



  }

  override def commit(): WriterCommitMessage = {
    connection.commit()
    SuccessfulCommit(jobId, partitionId)
  }

  override def abort(): Unit = {
    connection.rollback()
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