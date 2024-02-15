package localT

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import common.Constants.{Affichage}
import common.Utils.readCsv


class MainActor extends Actor {
  override def receive: Receive = {
    case _: HttpRequest =>
      sender ! HttpResponse(entity = "ok")
  }
}

object testInterface extends App {

  implicit val system = ActorSystem("HelloSystem") // Crée un système Akka
  implicit val materializer = ActorMaterializer() // Crée un materializer pour gérer les flux Akka HTTP
  implicit val executionContext = system.dispatcher // Un contexte d'exécution implicite pour exécuter les futures
  
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

  val df_driver  = readCsv(true,",",path_drivers).drop("driverId","url")
  val df_circuit = readCsv(true,",",path_circuits).drop("circuitId","lat","lng","alt","url")
  val df_team = readCsv(true,",",path_constructors).drop("constructorId","url")


  val htmttable_driver = Affichage(df_driver)
  val htmltable_circuit = Affichage(df_circuit)
  val htmltable_team = Affichage(df_team)

  // Charger le fichier HTML
  val htmlFile = scala.io.Source.fromFile("src/main/resources/index.html").mkString

  // Remplacer le commentaire dans le fichier HTML par le contenu du tableau
  val circuitHtml = htmlFile.replace("<!-- Vos données seront insérées ici -->", htmltable_circuit)
    .replace("<!-- vide -->","Liste Des Circuits De tous les temps")
  val driverhtml = htmlFile.replace("<!-- Vos données seront insérées ici -->", htmttable_driver)
    .replace("<!-- vide -->","Liste Des Pilotes De tous les temps ")
  val teamhtml = htmlFile.replace("<!-- Vos données seront insérées ici -->", htmltable_team)
    .replace("<!-- vide -->","Liste Des Equipes De tous les temps ")

  val mainActor = system.actorOf(Props[MainActor], "mainActor") // Crée un acteur principal

  val httpBinding = Http().newServerAt("localhost", 8081).bindFlow(
    path("") { // Cette directive correspond à l'URL racine "/"
        get { // Cette directive correspond à la méthode HTTP GET
          getFromResource("ac.html")
        }
      }
    ~
      path("circuit") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,circuitHtml ))
        }
      }
      ~
      path("driver") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,driverhtml ))
        }
      }
      ~
      path("team") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,teamhtml ))
        }
      }
      ~
      path("style.css") {
        get {
          getFromResource("style.css")
        }
      }
      ~
      path("script.js") {
        get {
          getFromResource("script.js")
        }
      }
      ~
      path("F1.svg.png") {
        get {
          getFromResource("F1.svg.png")
        }
      }
      ~
      path("ac.css") {
        get {
          getFromResource("ac.css")
        }
      }
      ~
      path("ac.js") {
        get {
          getFromResource("ac.js")
        }
      }
      ~
      path("F1_logo.svg") {
        get {
          getFromResource("F1_logo.svg")
        }
      }
      ~
      path("index.html") {
        get {
          getFromResource("index.html")
        }
      }
  )

  println(s"Server online at http://localhost:8081/\nPress RETURN to stop...")
  scala.io.StdIn.readLine() // Attend que l'utilisateur appuie sur Entrée

  httpBinding
    .flatMap(_.unbind()) // Arrête le serveur
    .onComplete(_ => system.terminate()) // Termine le système Akka
}
