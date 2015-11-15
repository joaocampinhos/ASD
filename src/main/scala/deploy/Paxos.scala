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

class littlePaxos() extends Actor {
  import context.dispatcher

  var toRespond: Option[ActorRef] = None
  var servers: HashMap[String, ActorRef] = HashMap[String, ActorRef]()

  def receive = {
    case Server.ServersConf(map) =>
      servers = map
    case Start(v) =>
      toRespond = Some(sender)
      self ! Learn(servers.values.head)
    case Learn(v) =>
      toRespond match {
        case Some(e) => e ! v
        case None => println("something is wrong")
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
    case Server.ServersConf(map) =>
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
