package com.ehaubert.imdb.rollup

import com.ehaubert.spark.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object ImdbRollupJob extends App {
  val dataDirectory = "dataset/"
  val fullPathToUnzippedFiles = MovieDataDownloader.downloadAndExtract(dataDirectory)

  val outputDirectory = "results/"

  val spark = SparkSessionProvider.sparkSession
  import spark.implicits._
  val CSV_READ_OPTIONS = Map(
    "header" -> "true",
    "delimiter" -> ",",
    "inferSchema" -> "true",
    //Fixme I came across bad CSV rows with newlines. I would circle back and scrub the data if this were not an exercise
    // decimals show up as dates for example
    "mode" -> "DROPMALFORMED",
    "parserLib" -> "univocity"
  )

  val movieMetaData = spark.read
    .options(CSV_READ_OPTIONS)
    .csv(s"$fullPathToUnzippedFiles/movies_metadata.csv")
    .withColumn("production_year", year($"release_date".cast(DateType).alias("production_year")))
    .collect()
    .map(r => (r.getAs[Int]("production_year"), r.getAs[String]("release_date")))
    .foreach(println)


}

