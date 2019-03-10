package org.tetra.data

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataSource {

  def getSourceData(sqlContext: SQLContext, config: Config, log: Logger): DataFrame = {
    var sourceData: DataFrame = null

    if (config.getBoolean("source.isDirectHive")) {
      log.info("Source Data Source : Hive Direct")
      sourceData = sqlContext.sql(config.getString("source.query"))
    } else if (config.getBoolean("source.isFile")) {
      log.info("Source Data Source : File")
      sourceData = getFileData(sqlContext, config.getString("source.fileName"), config.getBoolean("source.fileWithSchema"))
    } else {
      log.info("Source Data Source : JDBC")
      sourceData = getJdbcData(sqlContext,
        config.getString("source.databaseUrl"),
        config.getString("source.driverName"),
        config.getString("source.query"),
        config.getString("source.databaseUserName"),
        config.getString("source.databasePassword")
      )
    }

    return sourceData
  }

  def getTargetData(sqlContext: SQLContext, config: Config, log: Logger): DataFrame = {
    var targetData: DataFrame = null
    if (config.getBoolean("target.isDirectHive")) {
      log.info("Target Data Source : Hive Direct")
      targetData = sqlContext.sql(config.getString("target.query"))
    } else if (config.getBoolean("source.isFile")) {
      log.info("Source Data Source : File")
      targetData = getFileData(sqlContext, config.getString("target.fileName"), config.getBoolean("target.fileWithSchema"))
    } else {
      targetData = getJdbcData(sqlContext,
        config.getString("target.databaseUrl"),
        config.getString("target.driverName"),
        config.getString("target.query"),
        config.getString("target.databaseUserName"),
        config.getString("target.databasePassword")
      )
    }

    return targetData
  }

  def getJdbcData(sqlContext: SQLContext, dbUrl: String, driverName: String,
                  sqlQuery: String, userName: String, password: String): DataFrame = {

    return sqlContext.read.format("jdbc")
      .option("url", dbUrl)
      .option("driver", driverName)
      .option("dbtable", sqlQuery)
      .option("user", userName)
      .option("password", password)
      .load()

  }

  def getFileData(sqlContext: SQLContext, fileName: String, header: Boolean): DataFrame = {
    return sqlContext.read.format("csv")
      .option("header", header.toString)
      .load(fileName)
  }

}
