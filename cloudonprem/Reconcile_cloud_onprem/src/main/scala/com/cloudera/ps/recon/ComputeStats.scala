package com.cloudera.ps.recon
import com.cloudera.ps.CloudReconcile.{global_table_failure_list, logger, separator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count,lit}
import com.databricks.spark.avro._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer


class ComputeStats {

  def compute_write_stats(spark: SparkSession, src_system: String, country: String, azfs: AzureBlobFileSystemStore,
                          fs: FileSystem, dbName: String, table: String, src_path: String, staging_dir: String,
                          target_dir: String, table_type: String, data_format: String, partitionCol: String,
                          business_date: String, reconDB: String, reconTable: String, ctrl_file_path: String): Unit = {

    var counters: Int = 0
    var StagingPartFileCount: Int = -1
    var StagingPartSize: Long = -1
    var StagingPartRecordCount: Long = -1
    val source_path = s"$src_path + $separator + $partitionCol=$business_date"
    val target_path = s"$target_dir + $separator + $partitionCol=$business_date"
    val local_table_failure_list: ListBuffer[String] = new ListBuffer[String]()
    val staging_path: String = {
      if (staging_dir.nonEmpty && (staging_dir != "Plain")) s"$staging_dir + $separator + $partitionCol=$business_date"
      else "Plain"
    }

    try {
      azfs.listStatus(new Path(source_path))
    } catch {
      case e: AbfsRestOperationException =>
        if (e.getStatusCode == 404) {
          logger.warn(s"File Count: Partition data not found at source: $source_path for %s.%s: ".format(dbName, table))
          counters = -1
          logger.info(s"Mark all counters to %d".format(counters))
        } else {
          log_update_exception("FileCount", dbName, table, e.getMessage, local_table_failure_list)
        }
      case e: Throwable => log_update_exception("FileCount", dbName, table, e.getMessage, local_table_failure_list)
    }
    if (counters == -1) {
      write_to_recon_table(spark, src_system, country, dbName, table, -1, -1,
        -1, -1, -1, -1, -1,
        -1, -1, source_path, staging_path, target_path, "Done", business_date, reconDB, reconTable)
    } else {
      try {
        table_type match {
          case "t" => {
            logger.info("File Count: Table is of type tokenized")
            //val staging_path = s"$staging_dir + $separator + $partitionCol=$business_date"
            logger.info(s"File Count: Compute staging path file count: ${staging_path}")
            val allFileStatus: Array[FileStatus] = azfs.listStatus(new Path(staging_path))
            StagingPartFileCount = allFileStatus.length
            logger.info(s"FileCount: Staging File Count: $StagingPartFileCount")

            logger.info(s"PartitionSize: Compute staging path partition size: ${staging_path}")
            val allFileSizes: Array[Long] = allFileStatus.map(fileStatus => fileStatus.getLen)
            StagingPartSize = allFileSizes.sum
            logger.info(s"PartitionSize: Staging Partition Size: $StagingPartSize")

            logger.info(s"Record Count: Compute staging path record count: ${staging_path}")
            StagingPartRecordCount = spark
              .read
              .format(data_format)
              .load(s"${staging_path}")
              .count()
            logger.info(s"Record Count: Staging record count: ${StagingPartRecordCount}")
          }
          case "p" => {
            logger.info(s"File Count: Table %s.%s is of type plain".format(dbName, table))
            logger.info(s"File Count: Marked StagingPartFileCount to $StagingPartFileCount  " +
              s"StagingPartSize to $StagingPartSize StagingPartRecordCount to $StagingPartRecordCount")
          }
        }
        logger.info(s"File Count: Compute no.of source path file count: ${source_path}")
        val StagingSummary = fs.getContentSummary(new Path(source_path))
        val SourcePartFileCount = StagingSummary.getFileCount
        logger.info(s"File Count: Source Path File Count: $SourcePartFileCount")

        logger.info(s"PartitionSize: Compute source path partition size: ${source_path}")
        val SourcePartSize = StagingSummary.getLength
        logger.info(s"PartitionSize: Source path partition size: ${SourcePartSize}")

        logger.info(s"Record Count: Compute source path record count: ${source_path}")
        val SourcePartRecordCount = spark
          .read
          .format(data_format)
          .load(s"${source_path}")
          .count()
        logger.info(s"Record Count: Source path record count: ${SourcePartRecordCount}")

        logger.info(s"File Count: Compute target path file count: ${target_path}")
        val TargetSummary = fs.getContentSummary(new Path(target_path))
        val TargetPartFileCount = TargetSummary.getFileCount
        logger.info(s"File Count: Target path file count: ${TargetPartFileCount}")

        logger.info(s"PartitionSize: Compute target path partition size: ${target_path}")
        val TargetPartSize = TargetSummary.getLength
        logger.info(s"PartitionSize: Target path partition size: ${TargetPartSize}")

        logger.info(s"Record Count: Compute target path record count: ${target_path}")
        val TargetPartRecordCount = spark
          .read
          .format(data_format)
          .load(s"${target_path}")
          .count()
        logger.info(s"Record Count: Target path record count: ${TargetPartRecordCount}")

        if (local_table_failure_list.isEmpty) {
          val status = table_type match {
            case "t" => {
              if ((SourcePartRecordCount == StagingPartRecordCount) && (StagingPartRecordCount == TargetPartRecordCount)
                && (SourcePartFileCount == StagingPartFileCount)) "Done"
              else "Failed"
            }
            case "p" => {
              if ((SourcePartRecordCount == StagingPartRecordCount) &&
                (SourcePartRecordCount == TargetPartRecordCount)) "Done" else "Failed"
            }
          }
          write_to_recon_table(spark, src_system, country, dbName, table, SourcePartRecordCount, StagingPartRecordCount,
            TargetPartRecordCount, SourcePartFileCount, StagingPartFileCount, TargetPartFileCount, SourcePartSize,
            StagingPartSize, TargetPartSize, source_path, staging_path, target_path, status, business_date, reconDB, reconTable)
          write_control_file(spark, dbName, table, business_date, status, ctrl_file_path)
        }
      } catch {
        case e: Throwable => log_update_exception("ComputeStats", dbName, table, e.getMessage, local_table_failure_list)
      }
    }
  }

  def log_update_exception(Prefix: String, db: String, table: String, ErrorMessage: String,
                           local_table_failure_list: ListBuffer[String]): ListBuffer[String] = {
    val errorMessage = s"%s failed for table %s.%s due to %s".format(Prefix, db, table, ErrorMessage)
    logger.error(errorMessage)
    global_table_failure_list += (errorMessage)
    local_table_failure_list += s"%s.%s".format(db, table)
  }

  def log_hive_control_file_exception(Prefix: String, db: String, table: String, ErrorMessage: String): Unit = {

    val errorMessage = s"%s failed for table %s.%s due to %s".format(Prefix, db, table, ErrorMessage)
    logger.error(errorMessage)
    global_table_failure_list += (errorMessage)
  }

  def write_to_recon_table(spark: SparkSession, SrcSystem: String, Country: String, SchemaName: String, TableName: String,
                           SourcePartRecordCount: Long, StagingPartRecordCount: Long, TargetPartRecordCount: Long,
                           SourcePartFileCount: Long, StagingPartFileCount: Long, TargetPartFileCount: Long,
                           SourcePartSize: Long, StagingPartSize: Long, TargetPartSize: Long,
                           SourcePath: String, StagingPath: String, TargetPath: String, Status: String, partition_date: String,
                           reconDB: String, reconTable: String): Unit = {

    try {
      import spark.sqlContext.implicits._
      logger.info("HiveWriter:Preparing Recon DataFrame")
      val dummy = Seq(("dummy"))
      val dummy_DF: DataFrame = dummy.toDF("dummy")

      val pre_recon_df: DataFrame = dummy_DF.withColumn("SrcSystem", lit(SrcSystem))
        .withColumn("County", lit(Country))
        .withColumn("SchemaName", lit(SchemaName))
        .withColumn("TableName", lit(TableName))
        .withColumn("SourcePartRecordCount", lit(SourcePartRecordCount))
        .withColumn("StagingPartRecordCount", lit(StagingPartRecordCount))
        .withColumn("TargetPartRecordCount", lit(TargetPartRecordCount))
        .withColumn("SourcePartFileCount", lit(SourcePartFileCount))
        .withColumn("StagingPartFileCount", lit(StagingPartFileCount))
        .withColumn("TargetPartFileCount", lit(TargetPartFileCount))
        .withColumn("SourcePartSize", lit(SourcePartSize))
        .withColumn("StagingPartSize", lit(StagingPartSize))
        .withColumn("TargetPartSize", lit(TargetPartSize))
        .withColumn("SourcePath", lit(SourcePath))
        .withColumn("StagingPath", lit(StagingPath))
        .withColumn("TargetPath", lit(TargetPath))
        .withColumn("Status", lit(Status))
        .withColumn("partition_date", lit(partition_date))

      val recon_df: DataFrame = pre_recon_df.drop("dummy")
      val targetTable = s"$reconDB.$reconTable"
      logger.info(s"HiveWriter: Write counters for $SrcSystem $Country $SchemaName.$TableName to: $targetTable")
      recon_df.write.mode("Append").insertInto(targetTable)
    } catch {
      case e: Throwable => log_hive_control_file_exception("HiveWriter", SchemaName, TableName, e.getMessage)
    }
  }

  def write_control_file(spark: SparkSession, dbName: String, tableName: String, partition: String,
                         status: String, ctrl_file_path: String): Unit = {

    try{
      logger.info("ControlFile: Get hadoop conf")
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      logger.info("ControlFile: set ack file dir and filename")
      val ack_dir = s"$ctrl_file_path/global"
      val fileName = dbName + "_" + tableName + "_" + partition
      val hdfsDir = new Path(ack_dir)
      if (status == "Done") {
        val hdfsFile = new Path(s"${ack_dir}/${fileName}.match")
        logger.info(s"ControlFile: Creating $hdfsFile")
        if (fs.exists(hdfsDir)) {
          fs.create(hdfsFile, true)
        } else {
          fs.create(hdfsDir)
          fs.create(hdfsFile, true)
        }
      } else {
        val hdfsFile = new Path(s"${ack_dir}/${fileName}.nonmatch")
        logger.info(s"ControlFile: Creating $hdfsFile")
        if (fs.exists(hdfsDir)) {
          fs.create(hdfsFile, true)
        } else {
          fs.create(hdfsDir)
          fs.create(hdfsFile, true)
        }
      }

    } catch {
      case e: Throwable => log_hive_control_file_exception("ControlFileWriter", dbName, tableName, e.getMessage)
    }
  }
}