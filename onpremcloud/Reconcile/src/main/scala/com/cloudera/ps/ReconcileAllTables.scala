package com.cloudera.ps

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.{AbfsRestOperationException, AzureBlobFileSystemException}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import scala.collection.mutable.ListBuffer

object ReconcileAllTables {
  val HDFS_FILE_INPUT_FORMAT: String = "orc"
  //val HDFS_FILE_INPUT_FORMAT: String = "text"
  val HDFS_FILE_OUTPUT_FORMAT: String = "orc"
  val APP_NAME: String = "IncrementalRecon"
  val STATUS_DONE: String = "Done"
  val STATUS_FAILED: String = "Failed"


  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printf("usage: <source system> <country> <hdfs_parent_dir_xml> <abfs_prefix> <consumed_tables_xml>" +
        " <partition> <databases> <db_partition_map> <recount_db> <recount_table>")
      sys.exit(1)
    }

    val sourceSystem: String = args(0)
    val country: String = args(1)
    val hdfsParentDirXml: String = args(2)
    var azfsPrefix: String = args(3)

    val consumedTablesXml: String = args(4)
    val partition: String = args(5)
    val databases: String = args(6)
    val dbPartitionMapStr: String = args(7)
    val recountDB: String = args(8)
    val recountTable: String = args(9)

    if (databases == null || consumedTablesXml == null || dbPartitionMapStr == null
      || azfsPrefix == null || recountDB == null || recountTable == null) {
      printf("databases, consumedTablesXml, dbPartitionMap, azfs, recountDb, or recountTable is null")
      sys.exit(1)
    }
    @transient lazy val logger = Logger.getLogger(this.getClass.getName)

    logger.info(s"sourceSystem=$sourceSystem")
    logger.info(s"country=$country")
    logger.info(s"hdfsParentDirXml=$hdfsParentDirXml")
    logger.info(s"azfsPrefix=$azfsPrefix")
    logger.info(s"consumedTablesXml=$consumedTablesXml")
    logger.info(s"partition=$partition")
    logger.info(s"databases=$databases")
    logger.info(s"dbPartitionMapStr=$dbPartitionMapStr")
    logger.info(s"recountDB=$recountDB")
    logger.info(s"recountTable=$recountTable")

    val databasesArray = databases.split(",")
    val tablesArray = consumedTablesXml.split(",")


    val dbPartitionMap: Map[String, String] = dbPartitionMapStr.split(",").map(pair => {
      val Array(k, v) = pair.split(":")
      k.trim -> v
    }
    ).toMap

    // if last character is '/', remove it
    azfsPrefix = if (azfsPrefix.charAt(azfsPrefix.length - 1) == '/')
      azfsPrefix.substring(0, azfsPrefix.length - 1)
    else
      azfsPrefix

    // set global environment

    val spark = create_spark_context(APP_NAME, logger)
    val failureList: ListBuffer[String] = new ListBuffer[String]()

    var azfs: AzureBlobFileSystemStore = null
    try {
      azfs = new AzureBlobFileSystemStore(new URI(azfsPrefix),
        true, spark.sparkContext.hadoopConfiguration)
    }
    catch {
      case ex: IOException =>
        logger.error("Recon job cannot complete due to create AzureBlobFileSystemStore instance failed")
        ex.printStackTrace()
        spark.stop()
        sys.exit(1)
    }

    try {
      for (table <- tablesArray) {
        for (db <- databasesArray) {
          var sourceFileCount: Int = -1
          var sourcePartSize: Long = -1
          var sourceRecordCount: Long = -1L

          var targetFileCount: Int = -1
          var targetPartSize: Long = -1L
          var targetRecordCount: Long = -1L

          var partitionCol: String = ""
          var sourceFullPath: String = ""
          var targetFullPath: String = ""
          var status: String = ""

          try {
            partitionCol = dbPartitionMap.getOrElse(db.toLowerCase, "")
            if (partitionCol.equals("")) throw new RuntimeException(s"failed to get Column Map for db $db")

            logger.info("check source for db - " + db + ", table - " + table + ", partition - " + partition)

            // SourcePath (hive)
            sourceFullPath = hdfsParentDirXml + "/" + db + "/" + table + "/" + partitionCol + "=" + partition

            // get source file count - SourcePartFileCount (hive)
            val hdfsPath = new Path(sourceFullPath)
            val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
            logger.info("File Count: Initialize HadoopFS")
            try {
              logger.info(s"File Count: Compute no.of files of a partition in src path: $sourceFullPath")
              sourceFileCount = fs.listStatus(hdfsPath).length
              logger.info(s"sourceFileCount: $sourceFileCount")
            }
            catch {
              case _: FileNotFoundException => // file not found on HDFS
                throw ReconHDFSFileNotFoundException(s"HDFS file $sourceFullPath not found")
            }

            // get source file size - SourcePartSize (hive)
            try {
              val contentSummary = fs.getContentSummary(hdfsPath)
              sourcePartSize = contentSummary.getLength
              logger.info(s"SourcePartSize: $sourcePartSize")
            }
            catch {
              case e: IOException => // fail to get file size
                throw ReconHDFSRuntimeException(s"Failed to get the size of HDFS file $sourceFullPath; $e")
            }

            // ---------------------------------------------------------------------------------------------------- //

            // TargetPath (hive)
            targetFullPath = azfsPrefix + hdfsParentDirXml + "/" + db + "/" + table + "/" + partitionCol + "=" + partition
            logger.info(s"File Count: Compute no.of files of a partition in cloud path: $targetFullPath")

            try {

              // get target file count - TargetPartFileCount (hive)
              val path = new Path(targetFullPath)
              logger.info(s"Folder Size: Compute folder size in cloud path: $targetFullPath")

              val allFileStatus: Array[FileStatus] = azfs.listStatus(path)
              targetFileCount = allFileStatus.length
              logger.info(s"Target Path: $targetFullPath, Target File Count: $targetFileCount")

              val allFileSizes: Array[Long] = allFileStatus.map(fileStatus => fileStatus.getLen)
              targetPartSize = allFileSizes.sum
              logger.info(s"Target Path: $targetFullPath, Target Part Size: $targetPartSize")
            }
            catch {
              case e@(_: AbfsRestOperationException | _: AzureBlobFileSystemException | _: IOException) =>
                e.printStackTrace()
                throw ReconAzureReconRuntimeException(s"Failed to get information for Azure path: $targetFullPath; $e")
            }

            // count records from sources
            sourceRecordCount = spark
              .read
              .format(HDFS_FILE_INPUT_FORMAT)
              .load(sourceFullPath)
              .count()
            logger.info(s"Source record count: $sourceRecordCount")

            // count records from target
            logger.info(s"Record Count: Compute cloud path record counts: " + targetFullPath)
            targetRecordCount = spark
              .read
              .format(HDFS_FILE_INPUT_FORMAT)
              .load(targetFullPath)
              .count()
            logger.info(s"target $targetFullPath record count: $targetRecordCount")

            // check status
            status = if (sourceRecordCount == targetRecordCount) STATUS_DONE else STATUS_FAILED

            val curReconDF = spark.createDataFrame(Seq(
              ReconRecord(sourceSystem, country, db, table,
                sourceRecordCount.toString, targetRecordCount.toString,
                sourceFileCount.toString, targetFileCount.toString,
                sourcePartSize.toString, targetPartSize.toString,
                sourceFullPath, targetFullPath, status, partition)))
            curReconDF.show()


            spark.sql(s"""use $recountDB""")

            logger.info(s"write recon result into $recountDB.$recountTable")
            curReconDF.write.format(HDFS_FILE_OUTPUT_FORMAT).mode("Append").insertInto(recountDB + "." + recountTable)

            // generate control file
            // source record count  == target record count, then write match control file; otherwise, write nonmatch
            //     val dir_name = source_system + "/" + country + "/" + dbName + "/"
            //    val fileName = dbName + "_" + tableName + "_" + partition
            // check if dir exist, otherwise create the dir
            // val abs_file = new Path(s"${azfsPrefix}${dir_name}/${fileName}.match")
            // val abs_file = new Path(s"${azfsPrefix}${dir_name}/${fileName}.nonmatch")
            write_control_file(spark, azfs, azfsPrefix, sourceSystem, country, db, table, partition,
              sourceRecordCount == targetRecordCount, logger)
          }
          catch {
            case ex: ReconHDFSFileNotFoundException =>
              // handle error
              // 1) set to -1 for fileCount, recordCount and File size of source and target
              // 2) write to hive table
              // 3) since source record count  == target record count, then write match control file
              // 4) continue to next table
              logger.error(ex.explain)
              sourceFileCount = -1
              sourcePartSize = -1
              sourceRecordCount = -1
              targetFileCount = -1
              targetRecordCount = -1
              targetPartSize = -1
              status = STATUS_DONE

              //write result to Hive
              val curReconDF = spark.createDataFrame(Seq(
                ReconRecord(sourceSystem, country, db, table,
                  sourceRecordCount.toString, targetRecordCount.toString,
                  sourceFileCount.toString, targetFileCount.toString,
                  sourcePartSize.toString, targetPartSize.toString,
                  sourceFullPath, targetFullPath, status, partition)))
              logger.info(s"write recount result into $recountDB.$recountTable")

              curReconDF.write.format(HDFS_FILE_OUTPUT_FORMAT).mode("Append").insertInto(recountDB + "." + recountTable)


              // write control file
              write_control_file(spark, azfs, azfsPrefix, sourceSystem, country, db, table, partition,
                sourceRecordCount == targetRecordCount, logger)

            case ex: ReconHDFSRuntimeException =>
              // log exception
              // save to error table list
              // continue to next table
              logger.error(ex.explain)
              failureList += s"db: $db, table: $table, cause: ${ex.explain}"

            case ex: ReconAzureReconRuntimeException =>
              // log exception
              // add table to table failure list
              // continue to next table
              logger.error(ex.explain)
              failureList += s"db: $db, table: $table, cause: ${ex.explain}"
          } // end catch
        } // end for
      } //end for
    }
  catch
  {
    case ex: ReconControlFileRuntimeException =>
      logger.error("Recon job cannot complete due to write control file failed")
      ex.printStackTrace()
      spark.stop()
      sys.exit(1)

    case ex: Exception =>
      logger.error("Recon job cannot complete due to failure")
      ex.printStackTrace()
      spark.stop()
      sys.exit(1)
  }
  finally
  {
    if (failureList.nonEmpty) {
      logger.error("Recon job has completed with failures")
      for (failure <- failureList) {
        logger.error(failure)
      }

      spark.stop()

      sys.exit(1)
    } else {
      logger.info("Recon job succeed")

      spark.stop()

      //sys.exit(0)  - Donot enable: Job Fails with Race Condition
    }
  }

}

// create spark context
def create_spark_context (app_name: String, logger: Logger): SparkSession = {

  logger.info ("Creating spark session")

  val spark = SparkSession
  .builder ()
  .config ("hive.exec.dynamic.partition", "true")
  .config ("hive.exec.dynamic.partition.mode", "nonstrict")
  .appName (app_name)
  .enableHiveSupport ()
  .getOrCreate ()
  spark
  }

  // write control file
  def write_control_file (spark: SparkSession, azfs: AzureBlobFileSystemStore,
  azfsPrefix: String, sourceSystem: String,
  country: String, db: String,
  table: String, partition: String,
  isMatch: Boolean, logger: Logger
  ) = {
  val controlFileDir: String = azfsPrefix + "/" + sourceSystem + "/" + country + "/" + db
  val controlFileName: String = if (isMatch) db + "_" + table + "_" + partition + ".match"
  else
  db + "_" + table + "_" + partition + ".nonmatch"

  val controlFileFullPathStr = controlFileDir + "/" + controlFileName

  logger.info ("write control file " + controlFileFullPathStr)

  val controlFileDirPath = new Path (controlFileDir)
  val controlFileFullPath = new Path (controlFileFullPathStr)
  try {
  if (azfs == null) {
  throw ReconControlFileRuntimeException (s"Failed to write control file: $controlFileFullPathStr; AzureBlobFileSystemStore instance is null")
  }
  azfs.listStatus (controlFileDirPath).nonEmpty
  } catch {
  case e: AbfsRestOperationException =>
  e.printStackTrace ()
  azfs.createDirectory (controlFileDirPath, FsPermission.getDefault, FsPermission.getUMask (spark.sparkContext.hadoopConfiguration) )
  }

  try {
  azfs.createFile (controlFileFullPath, true, FsPermission.getDefault, FsPermission.getUMask (spark.sparkContext.hadoopConfiguration) )
  } catch {
  case e: AbfsRestOperationException =>
  throw ReconControlFileRuntimeException (s"Failed to write control file: $controlFileFullPathStr; $e")
  }

  }

  case class ReconRecord(srcSystem: String, country: String, schemaName: String, tableName: String,
                         sourcePartRecordCount: String, targetPartRecordCount: String,
                         sourcePartFileCount: String, targetPartFileCount: String,
                         sourcepartsize: String, targetpartsize: String,
                         sourcepath: String, targetpath: String,
                         status: String, partition_date: String
                        )

  case class ReconHDFSFileNotFoundException(explain: String) extends Exception(explain)

  case class ReconHDFSRuntimeException(explain: String) extends RuntimeException(explain)

  case class ReconAzureReconRuntimeException(explain: String) extends RuntimeException(explain)

  case class ReconControlFileRuntimeException(exception: String) extends RuntimeException(exception)
  }