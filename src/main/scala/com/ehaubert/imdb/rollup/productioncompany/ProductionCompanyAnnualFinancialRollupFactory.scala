package com.ehaubert.imdb.rollup.productioncompany

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProductionCompanyAnnualFinancialRollupFactory {
  def create(outputDirectory: String, productionCompanyPerMovieExplodedDF: DataFrame)(implicit spark: SparkSession): Unit ={
    import spark.implicits._

    productionCompanyPerMovieExplodedDF
      .groupBy($"production_company_id", $"production_year")
      .agg(
        first("production_company_name").as("production_company_name"),//I would want to validate that all data is actually the same here vs assuming
        sum($"budget").as("annual_budget"),
        sum($"revenue").as("annual_revenue"),
        sum($"revenue").minus(sum($"budget")).as("annual_profit"),
        avg($"popularity").as("average_movie_popularity")
      )
      .coalesce(1)
      .write
      .parquet(s"file://$outputDirectory/productionCompanyAnnualRollup")
  }
}
