package com.ehaubert.imdb.rollup

import java.io.File

import com.ehaubert.imdb.rollup.productioncompany.{MovieToProductionCompanyDataFrameFactory, ProductionCompanyAnnualFinancialRollupFactory, ProductionCompanyGenreRollupFactory}
import com.ehaubert.spark.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object ImdbRollupJob extends App with LazyLogging {
  val dataDirectory = "dataset/"
  val fullPathToUnzippedFiles = MovieDataDownloader.downloadAndExtract(dataDirectory)

  //Adding a little entropy here as multiple runs of this job might be something we want to keep around depending on
  //idempotency requirements
  val outputDirectory = new File(s"results/jobTimeStamp=${System.currentTimeMillis()}/").getAbsolutePath

  val t0 = System.currentTimeMillis()
  implicit val spark: SparkSession = SparkSessionProvider.sparkSession
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

  val movieMetaDataDF = spark.read
    .options(CSV_READ_OPTIONS)
    .csv(s"$fullPathToUnzippedFiles/movies_metadata.csv")
    .coalesce(1) //This data is so small let's not make this any more painful
    .withColumn("production_year", year($"release_date".cast(DateType).alias("production_year")))

  val productionCompanyPerMovieExplodedDF = MovieToProductionCompanyDataFrameFactory.create(movieMetaDataDF)
  ProductionCompanyAnnualFinancialRollupFactory.create(outputDirectory, productionCompanyPerMovieExplodedDF)
  ProductionCompanyGenreRollupFactory.create(productionCompanyPerMovieExplodedDF)
  logger.info(s"ETL completed in ${System.currentTimeMillis() - t0} ms")
}

