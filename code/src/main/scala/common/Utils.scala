package common

import common.Constants.spark
import org.apache.spark.sql.{DataFrame, SaveMode}
//import java.io.File
//import java.nio.file.{Files,  StandardCopyOption}
//import org.apache.hadoop.fs._
//import org.apache.hadoop.conf.Configuration
import scala.math._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object Utils {

// Fonction lecture de fichier csv

  def readCsv(header: Boolean, delimiter: String, path: String) = {
    spark.read
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)
  }


// Fonction d'ecriture de fichier csv

  def writeCsv(df:DataFrame,path:String) = {
   df.repartition(1).write.mode(SaveMode.Overwrite).csv(path)

  }


  def moveFile(source: String, oldFichier: String, destination: String, newFichier: String): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val cheminsource = new Path(source)
    val fichiers = fs.listStatus(cheminsource)
    val regex = oldFichier.r
    
    fichiers.foreach { fichier =>
      val fileName = fichier.getPath.getName
      if (regex.findFirstIn(fileName).isDefined) {
        val sortie = new Path(destination, newFichier)
        fs.rename(fichier.getPath, sortie)
        println(s"Le fichier $fileName a été déplacé avec succès vers $destination avec le nom $newFichier")
      }
    }


  }

    def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      //val earthRadius = 6371 // Rayon moyen de la Terre en kilomètres
      val earthRadius = 6378137 // Rayon moyen de la Terre en mètres
      val dLat = toRadians(lat2 - lat1)
      val dLon = toRadians(lon2 - lon1)
      val a = pow(sin(dLat / 2), 2) + cos(toRadians(lat1)) * cos(toRadians(lat2)) * pow(sin(dLon / 2), 2)
      val c = 2 * atan2(sqrt(a), sqrt(1 - a))
      val distance = earthRadius * c
      distance
    }

}
