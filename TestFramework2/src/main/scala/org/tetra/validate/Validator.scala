package org.tetra.validate

import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode}

class Validator {

  def getSchemaCompare(sourceDataFrame: DataFrame,targetDataFrame: DataFrame): Boolean = {
    val schemaCompared = getSchemaDifference(getCleanedSchema(sourceDataFrame),getCleanedSchema(targetDataFrame))
    if(schemaCompared.size >0){
      return false
    }else{
      return true
    }
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

  def getCleanedSchema(df: DataFrame): Map[String,DataType] = {
    df.schema.map{(structField: StructField) => structField.name.toLowerCase -> structField.dataType}.toMap
  }

  def getSchemaDifference(schema1: Map[String,DataType],schema2: Map[String,DataType]
                         ): Map[String, (Option[DataType], Option[DataType])] = {
    (schema1.keys ++ schema2.keys).
      map(_.toLowerCase).
      toList.distinct.
      flatMap { (columnName: String) =>
        val schema1FieldOpt: Option[DataType] = schema1.get(columnName)
        val schema2FieldOpt: Option[DataType] = schema2.get(columnName)

        if (schema1FieldOpt == schema2FieldOpt) None
        else Some(columnName -> (schema1FieldOpt, schema2FieldOpt))
      }.toMap
  }

}