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

    var paxos = context.actorOf(Props(new Paxos()), name = "Paxos")

    bota("Started")

    def bota(text: String) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def receive(): Receive = {
      case ServersConf(map) =>
        bota("Received addresses")
        serversAddresses = map
        paxos ! ServersConf(map)
        sender ! Success("Ok " + self.path.name)
        context.become(startJob(), discardOld = false)
      case _ => bota("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def startJob(): Receive = {
      case Alive => sender ! true
      case WhoIsLeader => actualLeader match {
        case Some(l) =>
          val clt = sender
          clt ! l
        // implicit val timeout = Timeout(180.seconds)
        // l ? Alive onComplete {
        //   case Success(result) =>
        //     bota("Leader alive: " + result)
        //     clt ! TheLeaderIs(l)
        //   case Failure(failure) =>
        //     bota("Leader alive: " + failure)
        //     electLeader(sender)
        // }
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
          // actualLeader = Some(result) //TODO UNCOMMENT THIS
          sender ! TheLeaderIs(result)
        case Success(result) =>
          bota("Future leader is " + result)
        case Failure(failure) =>
          bota("There is no leader in the Future")
          actualLeader = None
      }
    }
  }

  class Paxos() extends Actor {
    import context.dispatcher

    var count: Int = 0
    val myLearner = context.actorOf(Props(new Learner), name = "learner")
    val myAcceptor = context.actorOf(Props(new Acceptor), name = "acceptor")
    val myProposer = context.actorOf(Props(new Proposer), name = "proposer")

    val learners = new MutableList[ActorSelection]()
    val acceptors = new MutableList[ActorSelection]()
    val proposers = new MutableList[ActorSelection]()
    val paxos = new MutableList[ActorSelection]()

    var done = false

    var toRespond: Option[ActorRef] = None

    def receive = {
      case ServersConf(map) =>
        map.values.foreach(e => {
          proposers += context.actorSelection(e.path + "/Paxos/proposer")
          acceptors += context.actorSelection(e.path + "/Paxos/acceptor")
          learners += context.actorSelection(e.path + "/Paxos/learner")
          paxos += context.actorSelection(e.path + "/Paxos")
        })

        myProposer ! Servers(acceptors)
        myAcceptor ! Servers(learners)
        myLearner ! Servers(paxos)

      case Start(v) =>
        toRespond = Some(sender)
        myProposer ! Operation(v)
        myProposer ! Start

      case Learn(v) =>
        count = count + 1

        if (count == learners.size) {
          count = 0
          stopChild(myProposer, myAcceptor, myLearner, toRespond, v)

        }
    }

    def stopChild(child0: ActorRef, child1: ActorRef, child2: ActorRef, toRespond: Option[ActorRef], v: Any) = {
      implicit val timeout = Timeout(30 seconds)
      child0 ? Stop onComplete {
        case Success(result) =>
          println("Stop: " + result)
          child1 ? Stop onComplete {
            case Success(result) =>
              println("Stop: " + result)
              child2 ? Stop onComplete {
                case Success(result) =>
                  println("Stop: " + result)
                  toRespond match {
                    case Some(e) => e ! v
                    case None => println("something is wrong")
                  }
                case Failure(failure) =>
                  println("Failed to stop: " + failure)
              }

            case Failure(failure) =>
              println("Failed to stop: " + failure)
          }

        case Failure(failure) =>
          println("Failed to stop: " + failure)
      }

    }
  }
}
