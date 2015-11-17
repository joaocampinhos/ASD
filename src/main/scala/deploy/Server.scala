package deploy

import akka.actor._
import akka.remote.RemoteScope
import akka.event.Logging
import scala.collection.mutable.MutableList
import scala.collection.immutable._
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Failure
import scala.util.Success
import paxos._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

object Server {
  case class ServersConf(servers: HashMap[String, ActorRef])

  case class ServerActor() extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    var store = scala.collection.mutable.HashMap[String, String]()
    var serversAddresses = HashMap[String, ActorRef]()
    var actualLeader: Option[ActorRef] = None
    var alzheimer = true
    var debug = true

    var view = new View(0, MutableList[ActorRef](), MutableList[Action]())
    var paxos = context.actorOf(Props(new littlePaxos()), name = "Paxos")

    override def preStart(): Unit = {
        context.become(waitForData(), discardOld = false)
    }

    bota("Started")

    def bota(text: String) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def waitForData(): Receive = {
      case ServersConf(map) =>
        bota("Received addresses")
        serversAddresses = map
        paxos ! ServersConf(map)
        context.unbecome()
        sender ! Success("Ok " + self.path.name)
      case _ => bota("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def receive(): Receive = {
      case Alive =>
        bota("I'm alive")
        sender ! true
      case WhoIsLeader => actualLeader match {
        case Some(l) =>
          val clt = sender
          implicit val timeout = Timeout(180.seconds)
          l ? Alive onComplete {
            case Success(result) =>
              bota("Leader alive: " + result)
              clt ! TheLeaderIs(l)
            case Failure(failure) =>
              bota("Leader alive: " + failure)
              electLeader(clt)
          }
        case None =>
          electLeader(sender)
      }
      case Get(key) =>
        bota(key)
        sender ! Success("Get: " + key)
      case Put(key, value) =>
        bota(key + " " + value)
        sender ! Success("Put: " + key + ", " + value)
      case _ => bota("[Stage: Responding to Get/Put] Received unknown message.")
    }

    def electLeader(sender: ActorRef) = {
      implicit val timeout = Timeout(180.seconds)
      paxos ? Start(self) onComplete {
        case Success(result: ActorRef) =>
          bota("Future leader is " + result)
          actualLeader = Some(result)
          if (alzheimer) //TODO CAREFULL
            actualLeader = None
          sender ! TheLeaderIs(result)
        case Failure(failure) =>
          bota("There is no leader in the Future")
          actualLeader = None
      }
    }
  }
}
