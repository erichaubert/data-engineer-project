package com.ehaubert.imdb.rollup.genre

import com.ehaubert.imdb.rollup.ImdbJsonArraySchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GenreRollupFactory {

  def create(outputDirectory: String, rawMovieMetaDataDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    rawMovieMetaDataDF
      .withColumn("genre", explode(from_json($"genres", ImdbJsonArraySchema.SPARK_SCHEMA)))
      .groupBy($"genre.name".as("genre"), $"production_year")
      .agg(
        sum(lit(1)).as("total_movies"),
        sum($"budget").as("annual_budget"),
        sum($"revenue").as("annual_revenue"),
        sum($"revenue").minus(sum($"budget")).as("annual_profit")
      )
      .coalesce(1)
      .write
      .parquet(s"file://$outputDirectory/genreAnnualRollup")
  }
}
