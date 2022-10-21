package com.cloudera.ps
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import java.time.LocalDate
import org.apache.spark.sql.functions.lit

object AddPartitions{

  def create_spark_context(logger: Logger): SparkSession = {
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
    val sourceSystem: String = args(0)
    val country: String = args(1)
    val alter_statements: String = args(2)
    val status_dir: String = args(3)
    val partition = args(4)
    val day = LocalDate.now.getYear.toString + "%02d".format(LocalDate.now.getMonthValue) + LocalDate.now.getDayOfMonth.formatted("%02d")
    var tableName = ""
    val stat_array: Iterator[String] = alter_statements.split(",").toIterator
    @transient lazy val logger = Logger.getLogger(this.getClass.getName)
    logger.info(s"src:$sourceSystem")
    logger.info(s"cnt: $country")
    logger.info(s"alter:$alter_statements")
    logger.info(s"status:$status_dir")
    logger.info(s"part:$partition")
    logger.info(s"date:$day")
    val session = create_spark_context(logger)
    while (stat_array.hasNext) {
      try {
        val alter_stat = stat_array.next()
        val alter_statement = alter_stat.
          replace("[","").
          replace("\n","").
          replace("]","").
          replace(";","").replace(",","")
        logger.info(s"alter statement:$alter_statement")
        tableName = alter_statement.split(" ")(2)
        session.sql(s"$alter_statement")
      } catch {
        case ex: Throwable => {
          ex.printStackTrace()
          record_failure(session,tableName, ex)
        }
      }
    }
    def record_failure(spark:SparkSession, tbl: String, error: Throwable): Unit = {
      logger.error(s"Partition updated failed for :$sourceSystem $country $tbl")
      val exception = error.getMessage
      val message = s"Partition update failed for $sourceSystem $country $tbl due to:"
      import spark.sqlContext.implicits._
      val dummy = Seq(("dummy"))
      val dummy_df = dummy.toDF("dummy")
      val error_df = dummy_df.withColumn("message",lit(message))
                     .withColumn("exception",lit(exception)).drop("dummy")
      error_df.write.format("csv").mode("Overwrite").save(s"${status_dir}/MetaUpdate_${sourceSystem}_${country}_${tbl}_${partition}_failure.csv")
    }
  }
}