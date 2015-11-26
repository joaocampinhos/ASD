package deploy

import akka.event.Logging
import akka.actor.{ Actor, ActorRef }
import akka.pattern.ask
import akka.util.Timeout
import collection.mutable.{ MutableList, HashMap }
import concurrent.duration._
import concurrent.forkjoin.ThreadLocalRandom
import concurrent.{ Await, ExecutionContext, Future }
import util.{ Failure, Success }
import paxos.Start
import stats.Messages.{ ServerStart, ServerEnd }
import views.Views.{ OperationSuccessful, OperationError, Write, Read, UpdateView, View, JoinView, LeaderElected }

object Server {
  val MAX_HEARTBEAT_TIME = 2.seconds
  val MAX_ELECTION_TIME = 3.seconds

  abstract class Action
  case class Get(key: String) extends Action
  case class Put(key: String, value: String) extends Action
  case object SearchTimeout
  case object Stop
  case object WhoIsLeader
  case object Alive
  case class TheLeaderIs(l: ActorRef)
  case class ServersConf(servers: collection.mutable.HashMap[String, ActorRef])

  case class ServerActor(id: Int, paxos: ActorRef, paxosViews: ActorRef, stat: ActorRef) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    var serversAddresses = HashMap[String, ActorRef]()
    var actualLeader: Option[ActorRef] = None
    var alzheimer = false
    var debug = false

    // vars and vals
    var myViews = collection.mutable.Map[String, View]()
    var coordinator: ActorRef = null
    var serverId = id
    var isLeader = false; //highest server ID gets to be the leader
    var myCurrentViewId = 0
    var currentView = View(myCurrentViewId, self, List(), List())
    var kvStore = collection.mutable.Map[String, String]()
    var otherServers: Seq[ActorRef] = null
    var lastOperationId = 0
    var numSlaves = 0
    var quorumSize = -1

    stat ! ServerStart(self.path)

    override def preStart(): Unit = {
      context.become(waitForData(), discardOld = false)
    }

    override def postStop() {
      stat ! ServerEnd(self.path)
      log("Shuting down")
      debugLog(s"view(${currentView.id},${currentView.leader},${currentView.participants.map(e => e.path.name).toList})")
    }

    def log(text: Any) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }
    def debugLog(text: Any) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    var currentOperationId = 0 //TODO TO IMPROVE
    def getOperationId() = {
      currentOperationId += 1
      currentOperationId
    }

    def waitForData(): Receive = {
      case Stop =>
        context.stop(self)
      case ServersConf(map) =>
        coordinator = sender
        serversAddresses = map
        this.otherServers = map.values.filter(e => e != self).toSeq
        this.coordinator = coordinator
        val numServers = otherServers.size + 1
        if (numServers % 2 == 0) {
          this.quorumSize = otherServers.size / 2 + 1
        } else {
          this.quorumSize = otherServers.size / 2
        }
        paxos ! ServersConf(map)
        electLeaderThenAnswer(sender)
        context.unbecome()
        sender ! Success("Ok " + self.path.name)
        log("I'm ready")
      case _ => debugLog("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def receive(): Receive = {
      case Stop =>
        context.stop(self)
      case Alive =>
        debugLog("I'm alive")
        sender ! true
      case WhoIsLeader => heartbeatThenAnswer(sender)

      case UpdateView(view) => {
        if (isLeader) {
          // println("someone just told a leader to update their view.")
        } else {
          //just update the view man
          currentView = view
          debugLog(s"Replica received view state:${view.state}")
          // println(s"$self updated its view.")
          if (currentView.state.size > 0 && lastOperationId < currentView.state.last.id) {
            val tmpList = (lastOperationId to currentView.state.last.id - 1).map(e => currentView.state(e)).toList
            for (op <- tmpList)
              op match {
                case Read(_, key) =>
                  kvStore.get(key)
                case Write(_, key, value) =>
                  kvStore += (key -> value)
              }
            lastOperationId = currentView.state.last.id
          }
        }
      }

      case JoinView(serverId, who) => {
        // possible cases
        if (this.serverId < serverId) {
          // only possible if I already own a quorum
          if (isLeader) {
            println(s"Received request to join the quorum.")
          } else {
            println("impossible!")
          }
        } else {
          numSlaves += 1
          if (!isLeader && numSlaves >= quorumSize) {
            isLeader = true
            coordinator ! LeaderElected(self)
          }
        }
        // we always update the view
        myCurrentViewId = 1 //TODO
        currentView = View(myCurrentViewId, self, currentView.participants ::: List(who), currentView.state)
        currentView.participants.foreach(_ ! UpdateView(currentView)) // send to all participants
        println(s"View increased size by 1. Current leader is ${currentView.leader.path.name}")
      }
      //
      // client ops
      case op: Action => {
        if (isLeader) {
          myCurrentViewId = 1 //TODO
          var newOp = op match {
            case Get(key) => { Read(getOperationId(), key) }
            case Put(key, value) => { Write(getOperationId(), key, value) }
          }
          currentView = View(myCurrentViewId, currentView.leader, currentView.participants, currentView.state ::: List(newOp))
          implicit val timeout = Timeout(MAX_ELECTION_TIME)
          var clt = sender
          paxosViews ? Start(UpdateView(currentView)) onComplete {
            case Success(UpdateView(result)) =>
              debugLog("UpdateView by paxosViews: " + result)
              result.state.last match {
                case Read(_, key) => clt ! OperationSuccessful(s"Read($key)=${kvStore.get(key)}")
                case Write(_, key, value) =>
                  kvStore += (key -> value)
                  clt ! OperationSuccessful(s"Write($key,$value)")
              }
            case Success(result) => log("### Something went wrong ###ACTION: " + result)
            case Failure(failure) =>
              debugLog("UpdateView error by paxosViews: " + failure)
              clt ! OperationError(s"Op failed ")
          }
        }
      }
      case _ => debugLog("[Stage: Responding to Get/Put] Received unknown message.")
    }

    def heartbeatThenAnswer(respondTo: ActorRef) = {
      actualLeader match {
        case Some(l) =>
          if (l == self)
            respondTo ! TheLeaderIs(l)
          else {
            implicit val timeout = Timeout(MAX_HEARTBEAT_TIME)
            l ? Alive onComplete {
              case Success(result: Boolean) =>
                debugLog("Leader is alive: " + result)
                respondTo ! TheLeaderIs(l)
              case Success(result) => log("### Something went wrong ### HEARTBEAT: " + result)
              case Failure(failure) =>
                debugLog("Failure: " + failure)
                serversAddresses -= l.path.name
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
          log("Election: " + result.path.name)
          actualLeader = Some(result)
          isLeader = self == result
          currentView = View(1, result, currentView.participants ::: otherServers.toList, currentView.state)
          if (alzheimer) //TODO CAREFULL
            actualLeader = None
          sender ! TheLeaderIs(result)
        case Success(result) => log("### Something went wrong: ### ELECTION " + result)
        case Failure(failure) =>
          isLeader = false
          debugLog("Election failed: " + failure)
          actualLeader = None
      }
    }
  }
}
