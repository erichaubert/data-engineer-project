package com.ehaubert.imdb.rollup

object ImdbRollupJob extends App {
  val dataDirectory = "dataset/"
  MovieDataDownloader.downloadAndExtract(dataDirectory)

}

