package com.ehaubert.imdb.rollup.productioncompany

import com.ehaubert.imdb.rollup.ImdbRollupJob.productionCompanyPerMovieExplodedDF
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ProductionCompanyRollup {

//+---------------------+---------------+-------------------------------------+-------------+--------------+-------------+------------------+
  //|production_company_id|production_year|first(production_company_name, false)|annual_budget|annual_revenue|annual_profit|   avg(popularity)|
  //+---------------------+---------------+-------------------------------------+-------------+--------------+-------------+------------------+
  //|                    2|           2002|                 Walt Disney Pictures|    188000000|     377472863|    189472863|         8.2450715|
  //|                    5|           1995|                    Columbia Pictures|    170000000|     420751043|    250751043| 7.636466666666665|
  //|                   13|           2003|                    Universal Studios|            0|             0|            0|          0.769432|
  //|                   60|           1942|                       United Artists|            0|             0|            0|          3.803951|
  //|                   94|           2012|                   ARTE France Cinéma|            0|        148671|       148671|3.0679605000000003|

  def createGenreRollup(productionCompanyPerMovieExplodedDF: DataFrame)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    productionCompanyPerMovieExplodedDF
      .select($"production_company_id", $"production_company_name", $"production_year", $"genres")
      .show(100)

    productionCompanyPerMovieExplodedDF
      .select($"production_company_id", $"production_company_name", $"production_year", $"genres", lit(1).as("pivotCount"))
      .groupBy($"production_company_id", $"production_company_name", $"production_year")
      .pivot($"genre")
      .sum("pivot_count")
      .show(100)
  }
}
