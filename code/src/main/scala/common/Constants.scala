package common

import org.apache.spark.sql.{DataFrame, SparkSession}

object Constants {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark by groupe121")
    .getOrCreate()
  spark.conf.set("credentialsFile", "/home/ousseynou/Bureau/Atos/Big_Data/cloud/cloudysenegalcours-3788e56d0c3b.json")
  val chemin = "/home/ousseynou/Bureau/Atos/Big_Data/spark/Projet_locali/ressources/"
  spark.sparkContext.setLogLevel("ERROR")


  def Affichage(df:DataFrame): String ={
    val columns = df.columns
    val header = columns.map(column => s"<th>$column</th>").mkString("")

    val tableRows = df.collect().map { row =>
      val cells = row.toSeq.map(cell => s"<td>$cell</td>").mkString("")
      s"<tr>$cells</tr>"
    }

    val htmlTable = "<table>" + s"<tr>$header</tr>" + tableRows.mkString("") + "</table>"

    htmlTable
  }
}
