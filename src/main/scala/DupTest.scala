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

import org.apache.spark.sql.sources.v2.writer.DuplicateAwareDataSourceV2
import org.apache.spark.sql.sources.v2.writer.dupdetect.JdbcDuplicateDetection
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DupTest {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf
    sparkConf.setAppName("DupTest")
    sparkConf.setMaster("spark://mini1.marci.com:7077")
    //sparkConf.setMaster("local[4]")
    sparkConf.set("spark.hadoop.fs.default.name", "hdfs://mini1.marci.com:9000")
    sparkConf.setJars(Seq("hdfs://mini1.marci.com:9000/test/input/dadfw-0.1.0.jar,hdfs://mini1.marci.com:9000/postgresql-jdbc3.jar"))
    sparkConf.set("spark.logConf", "true")
    sparkConf.set("spark.executor.extraClassPath", "/usr/share/java/postgresql-jdbc3.jar")
    //sparkConf.set("spark.dynamicAllocation.enabled", "true")
    //sparkConf.set("spark.shuffle.service.enabled", "true")
    //sparkConf.set("spark.scheduler.mode", "FAIR")
    //sparkConf.set("spark.driver.supervise", "true")
    /*
    spark-submit --class DupTest --master spark://mini1.marci.com:7077 --deploy-mode cluster --conf spark.hadoop.fs.default.name=hdfs://mini1.marci.com:9000 hdfs://mini1.marci.com:9000/test/input/dadfw-0.1.0.jar
     */

    val sc = new SparkContext(sparkConf)
    val session: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    import session.implicits._

    val url = "jdbc:postgresql://node1.marci.com:5432/marci"

    println("Starting to read from file ...")

    session.read.textFile("/test/input/allwords.txt")
      .repartition(7)
      .map(word => Word(word, word.length))
      .write
      .mode(SaveMode.Append)
      .option("url", url)
      .option("dbtable", "words")
      .option("Driver", "org.postgresql.Driver")
      .option("user", "galyo")
      .option("password", "12345")
      .option("batchsize", "1000")
      .option(DuplicateAwareDataSourceV2.DUPLICATE_DETECTION, classOf[JdbcDuplicateDetection].getName)
      .format("org.apache.spark.sql.sources.v2.writer.DuplicateAwareJdbcDataSourceV2")
      .save()


    sc.stop()

    println("Finished ...")

  }

}

case class Word(word: String, len: Int)