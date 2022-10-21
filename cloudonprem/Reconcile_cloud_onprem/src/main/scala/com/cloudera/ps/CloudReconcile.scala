package com.cloudera.ps
import org.apache.spark.sql.SparkSession
import com.cloudera.ps.recon.ComputeStats
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore
import org.apache.log4j.Logger

import java.net.URI
import scala.collection.mutable.ListBuffer

object CloudReconcile {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)
  val separator = "/"
  val global_table_failure_list: ListBuffer[String] = new ListBuffer[String]()

  def create_spark_context(): SparkSession = {

    logger.info("Creating spark session")

    val spark = SparkSession
      .builder()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def main(args : Array[String]): Unit = {

    val src_system: String = args(0)
    val country: String = args(1)
    val abfs_container: String = args(2)
    val hdfs_parent_dir: String = args(3)
    val sri_open_staging_dir: String = args(4)
    val sri_non_open_staging_dir: String = args(5)
    val storage_staging_dir: String = args(6)
    val db_list: String = args(7)
    val tableList: String = args(8).replaceAll("\"","")
    val partitionColumnMap = args(9)
    val businessDate: String = args(10)
    val reconDB: String = args(11)
    val reconTable: String = args(12)
    val ctrl_file_path: String = args(13)

    val spark = create_spark_context()
    val cs = new ComputeStats()
    //val container = abfs_container.dropRight(1)
    logger.info("File Count: Initialize AzureFS")
    val azfs: AzureBlobFileSystemStore = new AzureBlobFileSystemStore(new URI(abfs_container), true,
      spark.sparkContext.hadoopConfiguration)
    logger.info("File Count: Initialize HadoopFS")
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    db_list.split(",").map(db => tableList.split(",").map(table => {

      val tableType: String = table.split("|").last.toLowerCase()
      val data_format: String = if (db.toLowerCase.contains("storage")) "com.databricks.spark.avro" else "orc"
      val cloud_src_table_location: String = abfs_container + hdfs_parent_dir + separator + db + separator + table
      val staging_dir: String = if(tableType == "t") {
        if (db.toLowerCase.equals("sri_open")) sri_open_staging_dir
        else if (db.toLowerCase.equals("sri_non_open")) sri_non_open_staging_dir
        else if (db.toLowerCase.equals("storage")) storage_staging_dir else ""
      }else if(tableType == "p") "Plain" else ""
      val partitionCol: String = if (db.toLowerCase.equals("sri_open")) {
        "ods"
      } else if (db.toLowerCase.equals("sri_non_open")) {
        "nds"
      } else if (db.toLowerCase.equals("storage")) {
        "vds"
      } else "None"
      val hdfs_staging_table_location = staging_dir + separator + table
      val hdfs_dest_table_location = hdfs_parent_dir + separator + db + separator + table
      logger.info("Compute File Count, FileSize & Record Count Statistic")
      cs.compute_write_stats(spark, src_system, country, azfs, fs, db, table, cloud_src_table_location, hdfs_staging_table_location, hdfs_dest_table_location,
        tableType, data_format, partitionCol, businessDate, reconDB, reconTable, ctrl_file_path)
      }))
    if (global_table_failure_list.nonEmpty){
      spark.stop()
      sys.exit(1)
    }else{
      spark.stop()
      sys.exit(0)
    }
  }
}