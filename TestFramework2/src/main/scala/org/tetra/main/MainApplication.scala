package org.tetra.main

import org.tetra.config.{ApplicationConfiguration, SparkSessionFactory}
import org.tetra.data.DataSource
import org.tetra.log.LoggingFactory
import org.tetra.validate.Validator

object MainApplication {

  def main(args: Array[String]): Unit = {
    val configFileName = args(0)
    val config = new ApplicationConfiguration().getConfigInfo(configFileName)
    val log = new LoggingFactory(config.getString("log.fileName")).getLogger()
    log.info("Job Executed by: "+config.getString("user.name"))
    log.info("Initiating Spark Session")
    val sqlContext = new SparkSessionFactory().getInstance()
    val validator = new Validator()
    val sourceDataFrame = new DataSource().getSourceData(sqlContext, config, log)
    val targetDataFrame = new DataSource().getTargetData(sqlContext,config,log)

    if(config.getBoolean("test.schemaCompare")){
      if(validator.getSchemaCompare(sourceDataFrame,targetDataFrame)){
        log.info("Schema Comparision Failed")
      }
    }
    if(config.getBoolean("test.rowCountCompare")){
      if(validator.getRowCountCheck(sourceDataFrame,targetDataFrame)){
        log.info("Rouw count check Failed")
      }
    }
    if(config.getBoolean("test.dataCompare")){
      if(validator.getDataCompare(sourceDataFrame,targetDataFrame,config.getString("dataCompareFileName"))){
        log.info("Data Comparision failed, result data found at: "+config.getString("dataCompareFileName"))
      }
    }

    sqlContext.sparkContext.stop()
  }
}
