package com.ehaubert.imdb.rollup

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.types.DateType

object MovieMetadataDataFrameFactory {

  def create(rawMoviesMetadataPath: String)(implicit spark: SparkSession): DataFrame ={
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

    spark.read
      .options(CSV_READ_OPTIONS)
      .csv(rawMoviesMetadataPath)
      .coalesce(1) //This data is so small let's not make this any more painful
      .withColumn("production_year", year($"release_date".cast(DateType).alias("production_year")))
  }
}
