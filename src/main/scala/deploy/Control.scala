package deploy

import collection.JavaConversions._
import akka.actor.ActorSystem
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import deploy.Server.Stop

object Control { //TODO to improve

  def main(args: Array[String]) {

    val serversIdxToKill = args.toList

    val config = ConfigFactory.load("control")

    val controlSystem = ActorSystem(config.getString("control.name"), config)
    val statURI = config.getString("stat.path")

    val serversURI = config.getStringList("controlToServer")
      .toList

    sendMsgs(serversIdxToKill, controlSystem, serversURI, statURI)
    // controlSystem.shutdown()
  }

  def sendMsgs(list: List[String], system: ActorSystem, serversURI: List[String], statURI: String) = {
    list match {
      case Nil => Nil
      case "status" :: Nil => printStats(system, statURI)
      case "kill" :: tail => kill(tail, system, serversURI)
    }
  }

  def printStats(system: ActorSystem, serversURI: String) = {
    system.actorSelection(serversURI + "/user/stat") ! "printStats"
  }

  def kill(serversIdxToKill: List[String], system: ActorSystem, serversURI: List[String]) = {
    println("Will kill servers: " + serversIdxToKill.mkString(", "))
    serversIdxToKill.foreach(idx => {
      val msg = idx match {
        case "stat" => "end"
        case _ => Stop
      }
      serversURI.foreach(e => system.actorSelection(e + "/user/Server" + idx) ! msg)
    })
  }
}
