package com.ehaubert.imdb.rollup.productioncompany

import com.ehaubert.imdb.rollup.ImdbJsonArraySchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{explode, from_json, lit}

object ProductionCompanyGenreRollupFactory {

  def create(outputDirectory: String, productionCompanyPerMovieExplodedDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    productionCompanyPerMovieExplodedDF
      .withColumn("genre", explode(from_json($"genres", ImdbJsonArraySchema.SPARK_SCHEMA)))
      .select(
        $"production_company_id",
        $"production_company_name",
        $"production_year",
        $"genre.name".as("genre"),
        lit(1L)as("pivot_count")//Since we aren't calculating a sum of revenue or anything, using a simple lit to sum counts
      )
      .groupBy($"production_company_id", $"production_company_name", $"production_year")
      .pivot($"genre")
      .sum("pivot_count")
      .coalesce(1)
      .write
      .parquet(s"file://$outputDirectory/productionCompanyAnnualGenreRollup")
  }
}
