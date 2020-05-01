package com.ehaubert.spark

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionProvider {

  lazy val sparkSession: SparkSession = {
    val hadoopStubHomeDir = new File(".")
    System.setProperty("hadoop.home.dir", hadoopStubHomeDir.getAbsolutePath)
    System.setProperty("java.version", "1.8    ") //hack to get java 1.14 working on my laptop
    val cfg = new SparkConf().setAppName("test").setMaster("local[*]")
    val ctx = new SparkContext(cfg)
    SparkSession
      .builder()
      .getOrCreate()
  }
}
