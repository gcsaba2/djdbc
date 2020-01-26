import java.sql.Connection

import org.apache.spark.sql.sources.v2.writer.DuplicateDetection

class DupDetectConn extends DuplicateDetection[Connection] {

  override def commitPartitionId(jobId: String, partitionId: Int, writer: Connection): Unit = {

  }

}
