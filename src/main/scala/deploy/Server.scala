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
  val MAX_HEARTBEAT_TIME = 180.seconds
  val MAX_ELECTION_TIME = 180.seconds

  case class ServersConf(servers: HashMap[String, ActorRef])

  case class ServerActor(paxos: ActorRef) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    var store = scala.collection.mutable.HashMap[String, String]()
    var serversAddresses = HashMap[String, ActorRef]()
    var actualLeader: Option[ActorRef] = None
    var alzheimer = true
    var debug = true

    override def preStart(): Unit = {
      context.become(waitForData(), discardOld = false)
    }

    bota("Started")

    def bota(text: String) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def waitForData(): Receive = {
      case ServersConf(map) =>
        serversAddresses = map
        paxos ! ServersConf(map)
        context.unbecome()
        sender ! Success("Ok " + self.path.name)
        bota("I'm ready")
      case _ => bota("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def receive(): Receive = {
      case Alive =>
        bota("I'm alive")
        sender ! true
      case WhoIsLeader => heartbeatThenAnswer(sender)
      case Get(key) =>
        val result = store.get(key)
        bota("Get:(" + key + "," + result + ")")
        sender ! Success(result)
      case Put(key, value) =>
        store += (key -> value)
        bota(key + " " + value)
        sender ! Success("Put: " + key + ", " + value)
      case _ => bota("[Stage: Responding to Get/Put] Received unknown message.")
    }

    def heartbeatThenAnswer(respondTo: ActorRef) = {
      actualLeader match {
        case Some(l) =>
          if (l == self)
            respondTo ! TheLeaderIs(l)
          else {
            implicit val timeout = Timeout(MAX_HEARTBEAT_TIME)
            l ? Alive onComplete {
              case Success(result) =>
                bota("Leader is alive: " + result)
                respondTo ! TheLeaderIs(l)
              case Failure(failure) =>
                bota("Failure: " + failure)
                electLeaderThenAnswer(respondTo)
            }
          }
        case None =>
          electLeaderThenAnswer(respondTo)
      }
    }

    def electLeaderThenAnswer(sender: ActorRef) = {
      implicit val timeout = Timeout(MAX_ELECTION_TIME)
      paxos ? Start(self) onComplete {
        case Success(result: ActorRef) =>
          bota("Election: " + result.path.name)
          actualLeader = Some(result)
          if (alzheimer) //TODO CAREFULL
            actualLeader = None
          sender ! TheLeaderIs(result)
        case Failure(failure) =>
          bota("Election failed: " + failure)
          actualLeader = None
      }
    }
  }
}
