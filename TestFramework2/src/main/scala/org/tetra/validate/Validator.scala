package org.tetra.validate

import org.apache.spark.sql.{DataFrame, SaveMode}

class Validator {

  def getSchemaCompare(sourceDataFrame: DataFrame,targetDataFrame: DataFrame): Boolean = {

    var testState = false
    if(sourceDataFrame.schema == targetDataFrame.schema)
    {
      testState = true
    }

    return testState
  }

  def getRowCountCheck(sourceDataFrame: DataFrame, targetDataFrame:DataFrame): Boolean = {
    var testState = false
    val testCount = sourceDataFrame.count()
    val prodCount = targetDataFrame.count()
    if (testCount == prodCount){
      testState = true
    }

    return testState
  }

  def getDataCompare(sourceDataFrame: DataFrame, targetDataFrame : DataFrame, outputPath: String): Boolean = {
    var testState = true
    var resultData = sourceDataFrame.except(targetDataFrame)
    resultData.unionAll(targetDataFrame.except(sourceDataFrame))
    if(resultData.count()>0){
      testState= false
      writeToFile(resultData,outputPath,"csv")
    }
    return testState
  }


  /**
    * Function to write a dataframe content into file.
    *
    *
    * @param dataFrame input dataframe
    * @param path      output directory to write the file
    * @param format    format of output file
    */
  def writeToFile(dataFrame: DataFrame, path: String, format: String): Unit = {
    dataFrame.write.mode(SaveMode.Append).format(format).save(path)
  }

}