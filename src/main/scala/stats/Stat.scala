package stats

import akka.actor.ActorSystem
import akka.actor.ActorPath
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.event.Logging
import scala.concurrent.duration._
import java.io._
import scala.Console
import deploy.Client._

case object Messages {
  case class ClientStart(path: ActorPath)
  case class ClientEnd(path: ActorPath)
  case class ServerStart(path: ActorPath)
  case class ServerEnd(path: ActorPath)
  case class StatOp(path: akka.actor.ActorPath, op: String)
}

case class State(init: Long, s: Boolean) {

  // Cliente ou servidor
  var server: Boolean = s

  // Quando o actor foi criado
  var start = init

  // Client -> Quando terminou a execucao
  // Server -> Quando crashou
  var end: Long = 0

  // Client -> Número de gets
  // Server -> Número de reads
  var gets = 0

  // Client -> Número de puts
  // Server -> Número de writes
  var puts = 0

  // Relevante apenas para clientes
  //----------

  //gets com sucesso
  var getsuc = 0

  //puts com sucesso
  var putsuc = 0

  // inicio do get actual
  var gettime: Long = 0
  // lista com todos os tempos de todos os gets
  var gettimes: List[Long] = List()

  // inicio do put actual
  var puttime: Long = 0
  // lista com todos os tempos de todos os puts
  var puttimes: List[Long] = List()

  // Relevante apenas para servidores
  //----------

  // Número de readTags
  var readtag = 0

}

case class StatActor() extends Actor {

  import context.dispatcher
  val log = Logging(context.system, this)

  var debug = false
  var start: Long = 0
  var end: Long = 0
  var stat: Map[akka.actor.ActorPath, State] = Map()

  // tempo de execucao do sistema -> Não funciona com a implementação actual
  // tempo de execucao de cada cliente -> Não funciona com a implementação actual
  // tempo de execucao medio de get -> implementado mas não testado
  // tempo de execucao medio de put -> implementado mas não testado
  // dump para ficheiro -> TODO

  def receive = {
    case "start" =>
      start = java.lang.System.currentTimeMillis()
      log("Started!")
    case "end" =>
      end = java.lang.System.currentTimeMillis()
      log("Will dump csv file and terminate!")
      dump()
      context.stop(self)
    case "printStats" =>
      end = java.lang.System.currentTimeMillis() //TODO CHECK
      log("Will dump csv file!")
      dump()
    case Messages.ClientStart(path) =>
      stat += (path -> new State(java.lang.System.currentTimeMillis(), false))
      debugLog("Client start => " + path)
    case Messages.ClientEnd(path) =>
      stat(path).end = java.lang.System.currentTimeMillis()
    case Messages.ServerStart(path) =>
      stat += (path -> new State(java.lang.System.currentTimeMillis(), true))
      debugLog("Server start => " + path)
    case Messages.ServerEnd(path) =>
      stat(path).end = java.lang.System.currentTimeMillis()
    case Messages.StatOp(path, op) => op match {
      case "get" =>
        stat(path).gets += 1
        if (!stat(path).server) stat(path).gettime = java.lang.System.currentTimeMillis()
        debugLog("get: " + path.toString)
      case "getsuc" =>
        stat(path).getsuc += 1
        if (!stat(path).server) stat(path).gettimes = java.lang.System.currentTimeMillis() - stat(path).gettime :: stat(path).gettimes
        //debugLog (stat(path).gettimes.toString)
        debugLog("get success: " + path.toString)
      case "put" =>
        stat(path).puts += 1
        if (!stat(path).server) stat(path).puttime = java.lang.System.currentTimeMillis()
        debugLog("put: " + path.toString)
      case "putsuc" =>
        stat(path).putsuc += 1
        if (!stat(path).server) stat(path).puttimes = java.lang.System.currentTimeMillis() - stat(path).puttime :: stat(path).puttimes
        // debugLog (stat(path).puttimes.toString)
        debugLog("put success: " + path.toString)
      case "readtag" =>
        stat(path).readtag += 1
        debugLog("readTag: " + path.toString)
      case _ => log.info("Unknown message. Ignoring")
    }
    case _ => log.info("Unknown message. Ignoring")
  }

  def log(text: Any) = { println(Console.BLUE + "[Stat] " + Console.GREEN + text + Console.WHITE) }

  def debugLog(text: Any) = { if (debug) println(Console.BLUE + "[Stat] " + Console.GREEN + text + Console.WHITE) }

  def dump() = {
    val writer = new PrintWriter(new File("serverclient.csv"))
    val writer2 = new PrintWriter(new File("times.csv"))
    writer.write("sep=,\n")
    writer2.write("sep=,\n")
    writer.write("Adress,isServer,execTime,#gets,#puts,#readtag,#getsuc,#putsuc\n")
    stat.foreach {
      x =>
        {
          writer.write(x._1 + "," + x._2.server + "," + (x._2.end - x._2.start) + "," + x._2.gets + "," + x._2.puts + "," + x._2.readtag + "," + x._2.getsuc + "," + x._2.putsuc + "\n")
          if (!x._2.server) {
            writer2.write(x._1.toString);
            writer2.write(",gets");
            x._2.gettimes.foreach { z => writer2.write("," + z.toString) }
            writer2.write("\n" + x._1.toString);
            writer2.write(",puts");
            x._2.puttimes.foreach { z => writer2.write("," + z.toString) }
          }
        }
    }
    writer.close()
    writer2.close()
  }
}
