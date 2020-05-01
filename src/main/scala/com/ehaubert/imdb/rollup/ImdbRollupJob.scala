package com.ehaubert.imdb.rollup

import java.io.File

import com.ehaubert.imdb.rollup.productioncompany.{MovieToProductionCompanyDataFrameFactory, ProductionCompanyAnnualFinancialRollupFactory, ProductionCompanyGenreRollupFactory}
import com.ehaubert.spark.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object ImdbRollupJob extends App with LazyLogging {
  val dataDirectory = "dataset/"
  val fullPathToUnzippedFiles = MovieDataDownloader.downloadAndExtract(dataDirectory)

  //Adding a little entropy here as multiple runs of this job might be something we want to keep around depending on
  //idempotency requirements
  val outputDirectory = new File(s"results/jobTimeStamp=${System.currentTimeMillis()}/").getAbsolutePath

  val t0 = System.currentTimeMillis()
  implicit val spark: SparkSession = SparkSessionProvider.sparkSession

  val movieMetaDataDF = MovieMetadataDataFrameFactory.create(s"$fullPathToUnzippedFiles/movies_metadata.csv")
  movieMetaDataDF.cache()
  val productionCompanyPerMovieExplodedDF = MovieToProductionCompanyDataFrameFactory.create(movieMetaDataDF)
  ProductionCompanyAnnualFinancialRollupFactory.create(outputDirectory, productionCompanyPerMovieExplodedDF)
  ProductionCompanyGenreRollupFactory.create(outputDirectory, productionCompanyPerMovieExplodedDF)
  movieMetaDataDF.unpersist()
  logger.info(s"ETL completed in ${System.currentTimeMillis() - t0} ms")
}

