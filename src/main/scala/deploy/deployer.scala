package deploy

import com.typesafe.config.{ ConfigFactory, Config }
import akka.actor.{ ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString }
import akka.remote.RemoteScope
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import collection.mutable.{ HashMap, MutableList }
import concurrent.Await
import concurrent.duration._
import util.{ Failure, Success }
import stats.StatActor
import deploy.Server.{ ServerActor, ServersConf ,Alive}
import deploy.Client.{ClientActor,IsAlive}
import paxos.{ ElectionPaxos, ViewPaxos }

object Deployer {
  case object Start
  def main(args: Array[String]) {
    val remoteSettings = args.toList
    val config = ConfigFactory.load("deployer")
    val system = ActorSystem(config.getString("deployer.name"), config)
    val actor = system.actorOf(Props(new DeployerActor(remoteSettings, config)), name = "D")
    actor ! Start
  }
  case class DeployerActor(remoteSettings: List[String], config: Config) extends Actor {
    import context.system

    var totalServers = config.getInt("totalServers")
    var totalClients = config.getInt("totalClients")
    var replicationDegree = config.getInt("replicationDegree")
    var stat = system.actorOf(Props(new StatActor()), name = "stat")
    var serversMap = HashMap[String, ActorRef]()
    var clientsMap = HashMap[String, ActorRef]()
    var debug = false
    var paxoslist = MutableList[ActorRef]()
    var paxosVlist = MutableList[ActorRef]()
    var aliveCounter = 0

    def log(text: Any) = { println(Console.RED + "[Deployer] " + Console.GREEN + text + Console.WHITE) }
    def debugLog(text: Any) = { if (debug) println(Console.RED + "[Deployer] " + Console.GREEN + text + Console.WHITE) }

    def receive(): Receive = {
      case Start => init()
      case _ => Unit
    }
    def waitForServerAndPaxos(): Receive = {
      case Alive =>
        aliveCounter += 1
        if (aliveCounter == totalServers) {
          deployClients(0 to (totalClients - 1))
          aliveCounter = 0
          context.become(waitForClients(), discardOld = true)
          clientsMap.values.foreach(e => e ! IsAlive)
        }
      case _ => Unit
    }
    def waitForClients(): Receive = {
      case Alive =>
        aliveCounter += 1
        if (aliveCounter == totalClients) {
          aliveCounter = 0
          context.unbecome()
          log("Deployment was successful.")
        }
      case _ => Unit
    }

    def init() = {
      stat ! "start"
      deployPaxosActors(0 to totalServers - 1)
      deployServers(0 to totalServers - 1)
      serversMap.values.map(e => {
        e ! ServersConf(serversMap, List() ++ paxoslist, List() ++ paxosVlist)
      })
      context.become(waitForServerAndPaxos(), discardOld = false)
    }

    def createServer(remotePath: String, serverIdx: Int): ActorRef = {
      system.actorOf(Props(classOf[ServerActor], serverIdx, stat, replicationDegree)
        .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(remotePath)))), "Server" + serverIdx)
    }

    def createClient(remotePath: String, clientIdx: Int): ActorRef = {
      system.actorOf(Props(classOf[ClientActor], serversMap, Client.parseClientConf(config), stat, replicationDegree)
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

    def deployPaxosActors(list: Range) = {
      list.map(e => (e, config.getString(generateDeployPath(e))))
        .map(e =>
          system.actorOf(Props(new ElectionPaxos(e._1, 3))
            .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(e._2)))), name = "Paxos" + (e._1)))
        .foreach(e => paxoslist += e)

      list.map(e => (e, config.getString(generateDeployPath(e))))
        .map(e =>
          system.actorOf(Props(new ViewPaxos(e._1, 3))
            .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(e._2)))), name = "PaxosV" + (e._1)))
        .foreach(e => paxosVlist += e)

    }

    def generateDeployPath(idx: Int): String = {
      remoteSettings(idx % remoteSettings.size) + ".path"
    }
  }

}
