package com.ehaubert.imdb.rollup

import java.io.File
import java.net.URL
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipFile, ZipInputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils

object MovieDataDownloader extends LazyLogging {

  def downloadAndExtract(targetPath: String): Unit = {
    val targetPathFile = new File(targetPath)
    if(targetPathFile.exists() && targetPathFile.listFiles().length >0){
      logger.info(s"Files already exist. No reason to redownload them! files=${targetPathFile.listFiles().map(_.getName).mkString(",")}")
    } else {
      val zipTarget = new File("the-movies-dataset.zip")
      downloadFileIfNotExists(zipTarget)
      unzip(zipTarget, targetPathFile)
    }
  }

  private def downloadFileIfNotExists(zipTarget: File){
    if(zipTarget.exists()){
      logger.info(s"Zip file already present at ${zipTarget.getAbsolutePath}. No need to download")
    } else {
      logger.info(s"Downloading fresh zip to ${zipTarget.getAbsolutePath}")
      FileUtils.copyURLToFile(
        new URL("https://s3-us-west-2.amazonaws.com/com.guild.us-west-2.public-data/project-data/the-movies-dataset.zip"),
        zipTarget,
        3000,
        5000);
    }
  }

  private def unzip(toUnzip: File, destination: File): Unit = {
    destination.mkdirs()
    val zipFile = new ZipFile(toUnzip)

    import scala.collection.JavaConverters._
    zipFile
      .entries()
      .asScala
      .foreach(entry => Files.copy(zipFile.getInputStream(entry), destination.toPath.resolve(entry.getName)))
  }

}
