package deploy

import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap
import akka.actor.{ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString}
import akka.remote.RemoteScope
import akka.event.Logging
import deploy.Client.ClientActor

object Deployer {
  def main(args: Array[String]) {
    new Deployer(args.toList)
  }
}

class Deployer(remoteSettings: List[String]) {
  val config = ConfigFactory.load("deployer")
  val system = ActorSystem(config.getString("deployer.name"),config)
  var serversMap = HashMap[String,ActorRef]()
  var clientsMap = HashMap[String,ActorRef]()

  deploy(0 to config.getInt("totalServers") - 1, createServer, serversMap)
  deploy(0 to config.getInt("totalClients") - 1, createClient, clientsMap)

  def createServer(remotePath: String, serverIdx: Int) : ActorRef = {
    system.actorOf(Props(classOf[Server])
      .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(remotePath)))),"Server"+serverIdx)
  }

  def createClient(remotePath: String, clientIdx: Int) : ActorRef = {
    system.actorOf(Props(classOf[ClientActor],serversMap, Client.parseClientConf(config))
      .withDeploy(Deploy(scope = RemoteScope(AddressFromURIString(remotePath)))),"Client"+clientIdx)
  }

  def deploy(list: Range, callback: (String,Int) => ActorRef, map: HashMap[String,ActorRef]) = {
    list.map(e => (e,config.getString(generateDeployPath(e))))
      .map( tuple => (callback(tuple._2,tuple._1)))
      .foreach( e => map += (e.path.name -> e))
  }

  def generateDeployPath(idx: Int) : String ={
    remoteSettings(idx % remoteSettings.size) +".path"
  }
}

