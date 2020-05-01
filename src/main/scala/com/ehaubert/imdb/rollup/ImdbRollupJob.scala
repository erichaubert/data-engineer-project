package com.ehaubert.imdb.rollup

import java.io.File

import com.ehaubert.imdb.rollup.productioncompany.ProductionCompanyRollup
import com.ehaubert.spark.SparkSessionProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DateType, LongType, StringType, StructType}

object ImdbRollupJob extends App {
  val dataDirectory = "dataset/"
  val fullPathToUnzippedFiles = MovieDataDownloader.downloadAndExtract(dataDirectory)

  val outputDirectory = new File("results/").getAbsolutePath

  implicit val spark = SparkSessionProvider.sparkSession
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
    .withColumn("production_year", year($"release_date".cast(DateType).alias("production_year")))

  val jsonSchema = new ArrayType(
    elementType = new StructType()
      .add("id", LongType)
      .add("name", StringType),
    containsNull = false
  )

  val productionCompanyPerMovieExplodedDF = movieMetaDataDF.withColumn("production_company", explode(from_json($"production_companies", jsonSchema)))
    .select(
      $"production_company.id".as("production_company_id"),
      $"production_company.name".as("production_company_name"),
      $"production_year",
      $"revenue".cast(LongType),
      $"popularity",
      $"budget".cast(LongType),
      $"genres"
    )
    .filter($"production_company.id".isNotNull)
    .orderBy(s"production_company")

  productionCompanyPerMovieExplodedDF.show(100, false)

  val annualProductionCompanyDF = productionCompanyPerMovieExplodedDF.groupBy($"production_company_id", $"production_year")
    .agg(
      first("production_company_name").as("production_company_name"),//I would want to validate that all data is actually the same here vs assuming
      sum($"budget").as("annual_budget"),
      sum($"revenue").as("annual_revenue"),
      sum($"revenue").minus(sum($"budget")).as("annual_profit"),
      avg($"popularity").as("average_movie_popularity")
    )
  annualProductionCompanyDF.coalesce(1).write.parquet(s"file://$outputDirectory/productionComapnyAnnualRollup")

  ProductionCompanyRollup.createGenreRollup(productionCompanyPerMovieExplodedDF)

}

