package deploy

import com.typesafe.config.ConfigFactory
import akka.actor.{ ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString }
import akka.remote.RemoteScope
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import collection.mutable.HashMap
import concurrent.Await
import concurrent.duration._
import util.{ Failure, Success }
import stats.StatActor
import deploy.Server.{ ServerActor, ServersConf }
import deploy.Client.ClientActor
import paxos.{ PaxosActor, ViewsPaxos }

object Deployer {
  def main(args: Array[String]) {
    val remoteSettings = args.toList
    val config = ConfigFactory.load("deployer")
    val system = ActorSystem(config.getString("deployer.name"), config)
    var totalServers = config.getInt("totalServers")
    var paxos = system.actorOf(Props(new PaxosActor(totalServers)), name = "Paxos")
    var paxosV = system.actorOf(Props(new ViewsPaxos(3)), name = "PaxosViews")
    var stat = system.actorOf(Props(new StatActor()), name = "stat")
    var serversMap = HashMap[String, ActorRef]()
    var clientsMap = HashMap[String, ActorRef]()
    var debug = false

    stat ! "start"
    deployServers(0 to totalServers - 1)
    serversMap.values.map(e => {
      import scala.concurrent.ExecutionContext.Implicits.global
      implicit val timeout = Timeout(5 seconds)
      val future = e ? ServersConf(serversMap)
      future onComplete {
        case Success(result) => debugLog(result)
        case Failure(failure) => debugLog(failure)
      }
    })
    deployClients(0 to config.getInt("totalClients") - 1)
    log("Deployment was successful.")

    def log(text: Any) = { println(Console.RED + "[Deployer] " + Console.GREEN + text + Console.WHITE) }
    def debugLog(text: Any) = { if (debug) println(Console.RED + "[Deployer] " + Console.GREEN + text + Console.WHITE) }

    def createServer(remotePath: String, serverIdx: Int): ActorRef = {
      system.actorOf(Props(classOf[ServerActor], serverIdx, paxos, paxosV, stat)
        .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(remotePath)))), "Server" + serverIdx)
    }

    def createClient(remotePath: String, clientIdx: Int): ActorRef = {
      system.actorOf(Props(classOf[ClientActor], serversMap, Client.parseClientConf(config), stat)
        .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(remotePath)))), "Client" + clientIdx)
    }

    def deployServers(list: Range) = {
      list.map(e => (e, config.getString(generateDeployPath(e))))
        .map(tuple => (createServer(tuple._2, tuple._1)))
        .foreach(e => serversMap += e.path.name -> e)
    }

    def deployClients(list: Range) = {
      list.map(e => (e, config.getString(generateDeployPath(e))))
        .map(tuple => (createClient(tuple._2, tuple._1)))
        .foreach(e => clientsMap += e.path.name -> e)
    }
    def generateDeployPath(idx: Int): String = {
      remoteSettings(idx % remoteSettings.size) + ".path"
    }
  }
}
