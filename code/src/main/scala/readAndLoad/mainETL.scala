package readAndLoad
import com.google.cloud.spark.bigquery._
import common.Constants.spark
import org.apache.commons.net.ftp.{FTPClient,FTPCmd}
object mainETL extends App {
//https://console.cloud.google.com/bigquery?project=cloudysenegalcours
/*
  spark
    .read
    .option("parentProject", "cloudysenegalcours")
    .bigquery("bigquery-public-data:samples.shakespeare")
    .select("word")
    .where("word = 'Hamlet' or word = 'Claudius'")
    .show()
*/


  // Définir les paramètres FTP
  val ftpHost = "localhost"
  val ftpPort = 14148
  val ftpUsername = "ousseynou"
  val ftpPassword = "weuthie@(_"
  val ftpFilePath = "/home/ousseynou/Téléchargements/IdeGT/tmp/test_ftp/f1"

  // Initialiser un client FTP
  val ftpClient = new FTPClient()
  ftpClient.connect(ftpHost, ftpPort)
  ftpClient.login(ftpUsername, ftpPassword)

  // Supprimer le fichier
  val deletionSuccess = ftpClient.deleteFile(ftpFilePath)

  if (deletionSuccess) {
    println(s"Le fichier $ftpFilePath a été supprimé avec succès.")
  } else {
    println(s"Échec de la suppression du fichier $ftpFilePath.")
  }

  // Se déconnecter du serveur FTP
  ftpClient.logout()
  ftpClient.disconnect()
}

