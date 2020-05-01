package com.ehaubert.imdb.rollup.genre

import com.ehaubert.imdb.rollup.ImdbJsonArraySchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GenreRollupFactory {

  def create(outputDirectory: String, rawMovieMetaDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val result = rawMovieMetaDataDF
      .withColumn("genre", explode(from_json($"genres", ImdbJsonArraySchema.SPARK_SCHEMA)))
      .groupBy($"genre", $"production_year")
      .agg(
        sum(lit(1)),
        sum($"budget").as("annual_budget"),
        sum($"revenue").as("annual_revenue"),
        sum($"revenue").minus(sum($"budget")).as("annual_profit")
      )

    result
      .coalesce(1)
      .write
      .parquet(s"file://$outputDirectory/genreAnnualRollup")
  }
}
