package com.ehaubert.spark

import org.apache.spark.sql.SparkSession

class SparkSessionProvider {

  lazy val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("IMDB Rollup")
      .master("local[*]")
      .getOrCreate()
  }
}
