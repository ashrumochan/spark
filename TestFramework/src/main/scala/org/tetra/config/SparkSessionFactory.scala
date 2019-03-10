package org.tetra.config


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

class SparkSessionFactory {

  private var sqlContext: SQLContext = null

  def getInstance(): SQLContext = {
    if (sqlContext == null) {

      val conf = new SparkConf().setAppName("Data Validation")
      val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)

    }
    sqlContext
  }
}