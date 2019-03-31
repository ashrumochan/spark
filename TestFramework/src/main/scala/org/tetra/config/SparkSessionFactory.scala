package org.tetra.config

import org.apache.spark.sql.SparkSession}


class SparkSessionFactory {

  private var sparkSession: SparkSession = null

  def getInstance(): SparkSession = {
    if (sparkSession == null) {

      sparkSession = SparkSession
        .builder()
        .appName("Data Sanity Check")
        .getOrCreate()


    }
    sparkSession
  }
}