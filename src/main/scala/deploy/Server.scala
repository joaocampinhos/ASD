package deploy

import scala.collection.immutable._
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Failure
import scala.util.Success

object Server {
  case class ServersConf(servers: HashMap[String, ActorRef])

  case class ServerActor() extends Actor {
    import context.dispatcher
    val log = Logging(context.system, this)
    var store = scala.collection.mutable.HashMap[String, String]()
    var serversAddresses = HashMap[String, ActorRef]()
    var actualLeader: Option[ActorRef] = None
    bota("Started")

    def bota(text: String) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def receive(): Receive = {
      case ServersConf(map) =>
        bota("Received addresses")
        serversAddresses = map
        actualLeader = Some(serversAddresses.values.head)
        sender ! Success("Ok " + self.path.name)
        context.become(startJob(), discardOld = false)
      case _ => bota("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def startJob(): Receive = {
      case WhoIsLeader => actualLeader match {
        case Some(l) =>
          sender ! TheLeaderIs(l)
        case None =>
          sender ! TheLeaderIs(electLeader())
      }
      case Get(key) =>
        bota(key)
        sender ! Success("Get: " + key)
      case Put(key, value) =>
        bota(key + " " + value)
        sender ! Success("Put: " + key + ", " + value)
      case _ => bota("[Stage: Responding to Get/Put] Received unknown message.")
    }

    def electLeader(): ActorRef = {
      self //TODO Use paxos algorithm
    }
  }
}
