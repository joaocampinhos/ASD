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

    var paxos = context.actorOf(Props(new Paxost ()), name = "Paxos")

    bota("Started")

    def bota(text: String) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def receive(): Receive = {
      case ServersConf(map) =>
        bota("Received addresses")
        serversAddresses = map
        paxos ! ServersConf(map)
        // actualLeader = serversAddresses.get("Server0")//TODO REMOVE
        sender ! Success("Ok " + self.path.name)
        context.become(startJob(), discardOld = false)
      case _ => bota("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def startJob(): Receive = {
      case WhoIsLeader => actualLeader match {
        case Some(l) =>
          //TODO CONTACT LEADER, if it fails execute paxos
          sender ! TheLeaderIs(l)
        case None =>
          paxos ! Start(self)
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
      implicit val timeout = Timeout(5.seconds)
      paxos ? Start(self) onComplete {
        case Success(result: ActorRef) =>
          bota("Future leader is " + result)
          actualLeader = Some(result)
          sender ! TheLeaderIs(result)
        case Failure(failure) =>
          bota("There is no leader in the Furute")
          actualLeader = None
      }
    }
  }

  class Paxos() extends Actor {
    import context.dispatcher

    var count: Int = 0
    context.actorOf(Props(new Learner(self)), name = "learner")
    context.actorOf(Props(new Acceptor), name = "acceptor")
    context.actorOf(Props(new Proposer), name = "proposer")

    val learners = new MutableList[ActorSelection]()
    val acceptors = new MutableList[ActorSelection]()
    val proposers = new MutableList[ActorSelection]()

    //proposers.foreach(_ ! Debug)
    //acceptors.foreach(_ ! Debug)

    var done = false
    var toRespond: Option[ActorRef] = None

    def receive = {
      case ServersConf(map) =>
        map.values.foreach(e => {
          proposers += context.actorSelection(e.path + "/Paxos/proposer")
          acceptors += context.actorSelection(e.path + "/Paxos/acceptor")
          learners += context.actorSelection(e.path + "/Paxos/learner")
        })
        proposers.foreach(_ ! Servers(acceptors))
        acceptors.foreach(_ ! Servers(learners))

      case Start(v: ActorRef) =>
        println("Starting " + self.path)
        toRespond = Some(sender)
        proposers.foreach(e => e ! Operation(v))
        proposers.foreach(_ ! Start)

      case Learn(v) =>
        count = count + 1

        if (count == learners.size) {
          count = 0
          toRespond match {
            case Some(e) => e ! v
            case None => println("something is wrong")
          }
          for (p <- proposers) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
          for (p <- acceptors) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
          for (p <- learners) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
        }
    }
  }

}
