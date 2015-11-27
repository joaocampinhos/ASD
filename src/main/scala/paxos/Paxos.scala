package paxos

import akka.actor.{ Actor, ActorRef, Props }
import akka.remote.RemoteScope
import akka.event.Logging
import akka.util.Timeout
import akka.pattern.ask
import scala.collection.mutable.{ MutableList, HashMap }
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.Await
import akka.pattern.ask
import util.Failure
import util.Success
import views.Views.{ View, UpdateView }

class PaxosBase() extends Actor {
  import context.dispatcher

  var count: Int = 0
  val learners = MutableList[ActorRef]()
  val acceptors = MutableList[ActorRef]()
  val proposers = MutableList[ActorRef]()
  var toRespond: MutableList[ActorRef] = MutableList[ActorRef]()
  var debug = false
  var done = false

  def log(text: Any) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }
  def debugLog(text: Any) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

  def setupRoles() = {}

  def receive = {
    case any: Any => log("Received: " + any)
  }
}

class ViewsPaxos(id: Int, totalServers: Int) extends PaxosBase {
  import context.dispatcher
  setupRoles()

  override def setupRoles() = {
    proposers += context.actorOf(Props(new Proposer), name = "proposer")
    for (s <- 1 to totalServers) {
      acceptors += context.actorOf(Props(new Acceptor), name = "acceptor" + s)
    }
    learners += context.actorOf(Props(new Learner(self, totalServers)), name = "learner")

    proposers.foreach(_ ! Servers(acceptors))
    acceptors.foreach(_ ! Servers(learners))

    // proposers.foreach(_ ! Debug)
    // acceptors.foreach(_ ! Debug)
    // learners.foreach(_ ! Debug)
  }

  override def receive = {
    case Start(UpdateView(k, v)) =>
      debugLog("Started paxos for a view")
      toRespond = new MutableList() ++ v.participants
      toRespond += sender
      proposers(0) ! Operation(UpdateView(k, v))
      proposers(0) ! Go

    case Learn(v) =>
      count = count + 1
      if (count == learners.size) {
        debugLog("Learned: " + v)
        count = 0
        for (p <- proposers) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- acceptors) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- learners) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- toRespond) {
          p ! v
        }
        toRespond = new MutableList[ActorRef]()
      }
  }
}

class PaxosActor(id: Int, totalServers: Int) extends PaxosBase {
  import context.dispatcher
  setupRoles()
  override def setupRoles() = {
    for (s <- 1 to totalServers) {
      acceptors += context.actorOf(Props(new Acceptor), name = "acceptor" + s)
      proposers += context.actorOf(Props(new Proposer), name = "proposer" + s)
    }
    learners += context.actorOf(Props(new Learner(self, totalServers)), name = "learner")

    proposers.foreach(_ ! Servers(acceptors))
    acceptors.foreach(_ ! Servers(learners))

    // proposers.foreach(_ ! Debug)
    // acceptors.foreach(_ ! Debug)
    // learners.foreach(_ ! Debug)
  }

  override def receive = {
    case Start(v: Any) =>
      debugLog("Started paxos for a leader")
      if (toRespond.size < totalServers) {
        toRespond += sender
        proposers(toRespond.size - 1) ! Operation(v)
        if (toRespond.size == totalServers) {
          proposers.foreach(p => p ! Go)
        }
      }
    case Learn(v) =>
      count = count + 1
      if (count == learners.size) {
        debugLog("Learned: " + v)
        count = 0
        for (p <- proposers) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- acceptors) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- learners) {
          implicit val timeout = Timeout(50 seconds)
          val future = p ? Stop
          future onComplete {
            case Success(result) => debugLog(result)
            case Failure(failure) => debugLog(failure)
          }
        }
        for (p <- toRespond) {
          p ! (id, v)
        }
        toRespond = new MutableList[ActorRef]()
      }
  }
}
