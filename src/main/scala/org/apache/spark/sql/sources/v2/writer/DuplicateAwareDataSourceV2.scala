package org.apache.spark.sql.sources.v2.writer

import org.apache.spark.sql.sources.v2.{DataSourceV2, WriteSupport}

trait DuplicateAwareDataSourceV2 extends DataSourceV2 with WriteSupport {

}
