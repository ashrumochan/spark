from pyspark.sql import SparkSession
import json
import sys
import logging
import inspect
import os

def getConfig(filename):
    #data = None
    with open(filename) as configFile:
        data = json.load(configFile)

    return data

def getSource(spark, log, config):

    if(config['input']['isDirectHive']):
        log.info("Hive Direct Data Source")
        sourceData = spark.sql(config['input']['query'])
    elif (config['input']['isFile']):
        sourceData = spark.read.format("csv") \
            .option("header", config['input']['fileWithSchema']) \
            .load(config['input']['fileName'])
    else:
        log.info("JDBC Data Source")
        sourceData = spark.read.format("jdbc") \
            .option("url", config['input']['databaseUrl']) \
            .option("driver", config['input']['driverName']) \
            .option("dbtable", config['input']['query']) \
            .option("user", config['input']['databaseUserName']) \
            .option("password", config['input']['databasePassword']) \
            .load()
    return sourceData

def getLogger(logFilePath):

    function_name = inspect.stack()[1][3]
    logger = logging.getLogger(function_name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logFilePath)
    fh.setLevel(logging.DEBUG)
    fh_format = logging.Formatter('%(asctime)s - %(lineno)d - %(levelname)-8s - %(message)s')
    fh.setFormatter(fh_format)
    logger.addHandler(fh)

    return logger

def getSparkSession():
    spark = SparkSession.Builder \
        .appName("Data Validation") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.warehouse.data.skipTrash", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    return spark

def writeToFile(dataFrame, path, format):
    dataFrame.write.save(path=path,format=format,mode='append')

def absoluteDuplicate(sourceData,config,log):

    li = sourceData.columns
    resultData = sourceData.groupBy(li).count().filter("count>1")
    if(resultData.count()>0):
        log.error("Absolute Duplicate records found, data placed at: "+config['test']['absoluteDuplicateCheckFileName'])
        writeToFile(resultData,config['test']['absoluteDuplicateCheckFileName'],'csv')


def duplicateCheck(sourceData,config,log):
    inputColumns = config['test']['duplicateCheckColumns']
    cols = inputColumns.split(",")
    resultData = sourceData.groupBy(cols).count().filter("count>1")
    if (resultData.count() > 0):
        log.error("Duplicate records found, data placed at: " + config['test']['absoluteDuplicateCheckFileName'])
        writeToFile(resultData, config['test']['absoluteDuplicateCheckFileName'], 'csv')

def nullCheck(sourceData,config,log):
    cols = sourceData.columns
    for x in cols:
        if(sourceData.count() == sourceData.where(sourceData.col(x).isNull).count()):
            log.error(x + " is null")
        #else:
        #    print(x + " is not null")


if __name__ =='__main__':
    configFileName = sys.argv[1]
    config = getConfig(configFileName)
    log = getLogger(config['log']['fileName'])
    log.info("Data Sanity Check Started, executed by"+os.getlogin())
    spark = getSparkSession()
    sourceData = getSource(spark,config,log)
    if config['test']['duplicateCheck'] :
        duplicateCheck(sourceData,config,log)

    if config['test']['nullCheck'] :
        nullCheck(sourceData,config,log)

    if config['test']['absoluteDuplicateCheck'] :
        absoluteDuplicate(sourceData,config,log)

    log.info("Data Sanity check completed")