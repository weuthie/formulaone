package localT
import common.Constants.spark
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.functions.{col, input_file_name, lit, regexp_replace}

import java.time.{LocalDate, LocalDateTime}

object testMain extends App {
  import spark.implicits._
  def readCsv(header: Boolean, delimiter: String, path: String) = {
    spark.read
      .format("csv")
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)
  }

  /***

  val chemin = "/home/ousseynou/Bureau/Atos/Big_Data/formule_1/data/f1db_csv/"

val path_circuits = chemin + "circuits.csv"
  val path_driver_standings = chemin + "driver_standings.csv"
  val path_results = chemin + "results.csv"
  val path_constructor_results = chemin + "constructor_results.csv"
  val path_lap_times = chemin + "lap_times.csv"
  val path_seasons = chemin + "seasons.csv"
  val path_constructors = chemin + "constructors.csv"
  val path_pit_stops = chemin + "pit_stops.csv"
  val path_sprint_results = chemin + "sprint_results.csv"
  val path_constructor_standings = chemin + "constructor_standings.csv"
  val path_qualifying = chemin + "qualifying.csv"
  val path_status = chemin + "status.csv"
  val path_drivers = chemin + "drivers.csv"
  val path_races = chemin + "races.csv"

  val df_driver  = readCsv(true,",",path_drivers).show()
  val df_driver_standings = readCsv(true,",",path_driver_standings).filter("raceId=18").show()
  val df_driver_race = readCsv(true,",",path_results).filter("raceId=18").show(1000)
  val df_race = readCsv(true,",",path_races).show(1000)

*/
  try {
  // L'heure de début du traitement
  val heureDebut = java.sql.Timestamp.valueOf(LocalDateTime.now())
  println(heureDebut)
  val chemin = "/home/ousseynou/Bureau/Atos/Big_Data/ProjetFinal"
  val df_driver  = readCsv(true,";",chemin+"/SUDANI_DATA.csv")
  df_driver.printSchema()
  val dfWithUpdatedDates = df_driver.withColumn("date_id", regexp_replace(col("date_id"), "^\\d{4}", LocalDate.now().getYear.toString)).
    withColumn("calldate", regexp_replace(col("calldate"), "^\\d{4}", LocalDate.now().getYear.toString)).
    withColumn("calldate`(année)",lit(LocalDate.now().getYear.toString))

  dfWithUpdatedDates.show(false)

     // L'heure de fin du traitement
  val heureFin = java.sql.Timestamp.valueOf(LocalDateTime.now())
  println(heureFin)

  // spark.sql("SELECT input_file_name() as nomFichier").first().getString(0)
  // Nom du fichier
  val pathFichier = df_driver.inputFiles.headOption.getOrElse("").split("/").reverse(0)
  println(pathFichier)


  // Nombre de lignes dans le DataFrame
  val nombreLignes = df_driver.count()

  println(nombreLignes)
  // Nombre de colonnes dans le DataFrame
  val nombreColonnes = df_driver.columns.length
println(nombreColonnes)

  val dfWithoutDuplicates = df_driver.dropDuplicates()
  val nombreDoublons = nombreLignes - dfWithoutDuplicates.count()
println(nombreDoublons)


  // Affichez le DataFrame après la mise à jour

  // Créez un nouveau DataFrame avec les informations
  val infoDataFrame = Seq(
    (heureDebut, heureFin, pathFichier, nombreLignes, nombreColonnes, nombreDoublons)
  ).toDF("HeureDebut", "HeureFin", "NomFichier", "NombreLignes",  "NombreColonnes","nombreDoublons")

  // Enregistrez le nouveau DataFrame au format CSV
 // infoDataFrame.write.option("header", "true").csv(chemin + "/info_processing.csv")

  // Affichez le nouveau DataFrame
  infoDataFrame.show(false)

} catch {
  case e: org.apache.spark.sql.AnalysisException =>{
    println(s"Erreur d'analyse du DataFrame - ${e.getMessage}")
  }
  case e: Exception => {
    println(s"Erreur inattendue - ${e.getMessage}")
  }
}
}

