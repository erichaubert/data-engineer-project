package com.ehaubert.spark

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  lazy val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("IMDB Rollup")
      .master("local[*]")
      .getOrCreate()
  }
}
