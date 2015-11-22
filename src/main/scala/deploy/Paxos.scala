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
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

object Paxos {
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

  class PaxosActor(totalServers: Int) extends Actor {
    import context.dispatcher

    var count: Int = 0
    val learners = MutableList[ActorRef]()
    val acceptors = MutableList[ActorRef]()
    val proposers = MutableList[ActorRef]()

    for (s <- 1 to totalServers) {
      learners += context.actorOf(Props(new Learner(self)), name = "learner" + s)
      acceptors += context.actorOf(Props(new Acceptor), name = "acceptor" + s)
      proposers += context.actorOf(Props(new Proposer), name = "proposer" + s)
    }

    proposers.foreach(_ ! Servers(acceptors))
    acceptors.foreach(_ ! Servers(learners))

    var toRespond: MutableList[ActorRef] = MutableList[ActorRef]()

    var done = false

    def receive = {
      case Start(v) =>
        toRespond += sender
        proposers(toRespond.size - 1) ! Start(v)
        println(proposers.size)
        if (toRespond.size == totalServers){
          for (p <- proposers) {
            p ! Go
          }
        }
      case Learn(v) =>
        count = count + 1
        print("learned " + v)
        if (count == learners.size) {
          count = 0
          for (p <- proposers) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            future onComplete {
              case Success(result) => println(result)
              case Failure(failure) => println(failure)
            }
          }
          for (p <- acceptors) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            future onComplete {
              case Success(result) => println(result)
              case Failure(failure) => println(failure)
            }
          }
          for (p <- learners) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            future onComplete {
              case Success(result) => println(result)
              case Failure(failure) => println(failure)
            }
          }
          for (p <- toRespond) {
            p ! v
          }
        }

    }
  }
}
