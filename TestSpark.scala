package org.ashru
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestSpark {

  def main(args: Array[String]): Unit = {
    print("Hello")
    val conf = new SparkConf().setAppName("FirstApplication").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
/*    val spark = SparkSession
      .builder()
      .master("local")
      .appName("FirstApplication")
      .getOrCreate()*/
    import spark.implicits._

    /*val str = """{"id":"11","data":[{"name":"kkk","age":32},{"name":"BBB","age":34},{"name":"AAA","age":21},{"name":"ZZZ","age":45},{"name":"SSS","age":26}],"dob":[{"place":"hyd","time":"-5"},{"place":"blg","time":"-6"}]}"""
    val df = spark.read.option("inferschema",true).option("multiLine",true).json(Seq(str).toDS)*/
    sc.textFile("file:///D:/schema.txt").flatMap(x=>x.split(",")).map(x => x(0)).collect()

    val structure = spark.sparkContext.textFile("file:///D:/schema.txt").flatMap(x=>x.split(",")).collect.toSeq

    var origDf = spark.read.format("xml").option("rowTag","student").load("D:/old_student.xml").toDF(structure:_*)
    origDf = flattenDataframe(origDf)
    //origDf.show()

    var deltaDf = spark.read.format("xml").option("rowTag","student").load("D:/updated_student.xml").toDF(structure:_*)
    deltaDf  = flattenDataframe(deltaDf)
    //deltaDf.show()

    val departmentDf = spark.read.option("header","true").option("inferschema","true").csv("file:///D:/departments.csv")

    val origWithDepartment = origDf.as("orig").join(broadcast(departmentDf).as("dept"),$"orig.department"===$"dept.id")
      .select($"orig.id",$"orig.name",$"dept.name".as("department_name"))
    val deltaWithDepartment = deltaDf.as("delta").join(broadcast(departmentDf).as("dept"),$"delta.department"===$"dept.id")
      .select($"delta.id",$"delta.name",$"dept.name".as("department_name"))

    /*
    origWithDepartment.show()
    deltaWithDepartment.show()
    */

    //Implementation of SCD type-2
    val unchangedDf = origWithDepartment.as("orig").join(deltaWithDepartment.as("delta"),$"orig.id"===$"delta.id","leftanti")
                      .select($"orig.id",$"orig.name",$"orig.department_name")
    val finalDf = unchangedDf.union(deltaWithDepartment)

    finalDf.show()

  }

  def flattenDataframe(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case arrayType: ArrayType =>
          val fieldName_pos = fieldName+"_pos"
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"posexplode_outer($fieldName) as ($fieldName_pos,$fieldName)")
          val explodedDf = df.selectExpr (fieldNamesAndExplode: _*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldNames = structType.fieldNames.map(childName => fieldName + "." + childName)
          val newFieldNames = fieldNames.filter(_ != fieldName) ++ childFieldNames
          val renamedCols = newFieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replace("@", "").replace("%","").replace(" ", "").replace("-", ""))))
          val explodeDf = df.select(renamedCols: _*)
          return flattenDataframe(explodeDf)
        case _ =>
      }
    }
    df
  }

  def removeArrayColumns(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      fieldType match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val explodedDf = df.selectExpr (fieldNamesExcludingArray: _*)
          return removeArrayColumns(explodedDf)
        case _ =>
      }
    }
    df
  }
}
