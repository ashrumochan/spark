package org.tetra.main

import java.io.File

import com.typesafe.config.ConfigFactory
import org.tetra.config.ApplicationConfiguration

object LoggingTest {
  def main(args: Array[String]): Unit = {
    val configFile = new File("D:\\test.conf")

    val fileConfig = ConfigFactory.parseFile(configFile)
    val config = ConfigFactory.load(fileConfig)

    println(config)
    println(config.getBoolean("test.nullCheck"))
  }

}
