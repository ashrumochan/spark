package org.tetra.data

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataSource {

  def getSourceData(sparkSession: SparkSession, config: Config, log: Logger): DataFrame = {
    var sourceData: DataFrame = null

    if (config.getBoolean("source.isDirectHive")) {
      log.info("Source Data Source : Hive Direct")
      val query = sparkSession.sparkContext.wholeTextFiles(config.getString("source.queryFile")).take(1)(0)._2
      sourceData = sparkSession.sql(query)
    } else if (config.getBoolean("source.isFile")) {
      log.info("Source Data Source : File")
      sourceData = getFileData(sparkSession, config.getString("source.fileName"), config.getBoolean("source.fileWithSchema"))
    } else {
      log.info("Source Data Source : JDBC")
      sourceData = getJdbcData(sparkSession,
        config.getString("source.databaseUrl"),
        config.getString("source.driverName"),
        config.getString("source.query"),
        config.getString("source.databaseUserName"),
        config.getString("source.databasePassword")
      )
    }

    return sourceData
  }

  def getTargetData(sparkSession: SparkSession, config: Config, log: Logger): DataFrame = {
    var targetData: DataFrame = null
    if (config.getBoolean("target.isDirectHive")) {
      log.info("Target Data Source : Hive Direct")
      val query = sparkSession.sparkContext.wholeTextFiles(config.getString("target.queryFile")).take(1)(0)._2
      targetData = sparkSession.sql(query)
    } else if (config.getBoolean("source.isFile")) {
      log.info("Source Data Source : File")
      targetData = getFileData(sparkSession, config.getString("target.fileName"), config.getBoolean("target.fileWithSchema"))
    } else {
      targetData = getJdbcData(sparkSession,
        config.getString("target.databaseUrl"),
        config.getString("target.driverName"),
        config.getString("target.query"),
        config.getString("target.databaseUserName"),
        config.getString("target.databasePassword")
      )
    }

    return targetData
  }

  def getJdbcData(sparkSession: SparkSession, dbUrl: String, driverName: String,
                  sqlQuery: String, userName: String, password: String): DataFrame = {

    return sparkSession.read.format("jdbc")
      .option("url", dbUrl)
      .option("driver", driverName)
      .option("dbtable", sqlQuery)
      .option("user", userName)
      .option("password", password)
      .load()

  }

  def getFileData(sparkSession: SparkSession, fileName: String, header: Boolean): DataFrame = {
    return sparkSession.read.format("csv")
      .option("header", header.toString)
      .load(fileName)
  }

}
