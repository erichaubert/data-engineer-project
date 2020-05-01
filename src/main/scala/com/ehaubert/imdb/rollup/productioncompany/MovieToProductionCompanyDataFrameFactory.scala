package com.ehaubert.imdb.rollup.productioncompany

import com.ehaubert.imdb.rollup.ImdbJsonArraySchema
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieToProductionCompanyDataFrameFactory {

  def create(rawMovieMetaDataDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    rawMovieMetaDataDF
      .withColumn("production_company", explode(from_json($"production_companies", ImdbJsonArraySchema.SPARK_SCHEMA)))
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
  }
}
