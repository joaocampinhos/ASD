package deploy

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.ActorPath
import akka.actor.Props
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._
import java.io._
import scala.Console
import deploy.Client._

object Stat {
  case object Messages {
    case class ClientStart(path: ActorPath)
    case class ClientEnd(path: ActorPath)
    case class ServerStart(path: ActorPath)
    case class ServerEnd(path: ActorPath)
    case class StatOp(path: akka.actor.ActorPath, op: String)
    //
    case class Lat(path: akka.actor.ActorPath, v: Int)
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

    // Segundo trabalho
    //---------

    //Latencia total
    var lattotal: Int = 0

    //ops em geral
    var ops: Int = 0

  }

  case class StatActor() extends Actor {

    import context.dispatcher
    val log = Logging(context.system, this)

    var debug = false
    var start: Long = 0
    var end: Long = 0
    var stat: Map[akka.actor.ActorPath, State] = Map()

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
      case Messages.StatOp(path, op) => stat(path).ops += 1
      case Messages.Lat(path, v) => stat(path).lattotal = v
      case _ => log.info("Unknown message. Ignoring")
    }

    def log(text: Any) = { println(Console.BLUE + "[Stat] " + Console.GREEN + text + Console.WHITE) }

    def debugLog(text: Any) = {if(debug) println(Console.BLUE + "[Stat] " + Console.GREEN + text + Console.WHITE) }

    def dump() = {
      val config = ConfigFactory.load("deployer")
      val totalServers = config.getInt("totalServers")
      val totalClients = config.getInt("totalClients")
      val perReads = config.getInt("ratioOfReads")
      val writer = new PrintWriter(new File(f"s$totalServers%dc$totalClients%dr$perReads%d.txt"))
      writer.write("----------------------------\n")
      writer.write(f"Servidores   : $totalServers%d\n")
      writer.write(f"Clientes     : $totalClients%d\n")
      writer.write(f"%% Reads      : $perReads%d\n")
      writer.write("----------------------------\n")
      var throughput = 0
      var time:Long = 0
      var latency = 0
      stat.foreach {
        x =>
          if (!x._2.server) {
            //Tempo medio total (ms)
            time += (x._2.end - x._2.start)
            //thoughput (ops/segundo)
            val ops = x._2.ops
            throughput += (ops/(time.toFloat/1000)).toInt
            //latencia media total (ms)
            latency += x._2.lattotal
          }
      }
      writer.write(f"Tempo medio  : ${time.toFloat / totalClients}%.2f ms\n")
      writer.write(f"Throughput   : ${throughput / totalClients}%d ops/min\n")
      writer.write(f"Latencia     : ${latency / totalClients}%d ms\n")
      writer.close()
    }
  }
}
