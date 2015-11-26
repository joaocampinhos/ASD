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
  val MAX_EXEC_TIME = 5.seconds

  abstract class Action
  case class Get(key: String) extends Action
  case class Put(key: String, value: String) extends Action
  case object SearchTimeout
  case object Stop
  case object WhoIsLeader
  case object Alive
  case class TheLeaderIs(l: ActorRef)
  case class ServerDetails(keyHash: String, id: Int, view: View)
  case class JoinView(keyHash: String, id: Int, who: ActorRef)
  case class UpdateView(keyHash: String, view: View)
  case class ServersConf(servers: collection.mutable.HashMap[String, ActorRef])

  case class ServerActor(id: Int, paxos: ActorRef, paxosViews: ActorRef, stat: ActorRef) extends Actor {
    import context.dispatcher
    var replicationDegree = 2
    val log = Logging(context.system, this)
    var serversAddresses = HashMap[String, ActorRef]()
    var actualLeader: Option[ActorRef] = None
    var alzheimer = false
    var debug = false

    // vars and vals
    var coordinator: ActorRef = null
    var serverId = id
    // var isLeader = false; //highest server ID gets to be the leader
    // var myCurrentViewId = 0
    // var currentView = View(0, self, List(), List())
    var kvStore = collection.mutable.Map[String, String]()
    var otherServers: Seq[ActorRef] = null
    var lastOperationId = 0
    var numSlaves = 0
    var quorumSize = -1

    var viewsmap = HashMap[String, View]()
    stat ! ServerStart(self.path)

    override def preStart(): Unit = {
      context.become(waitForData(), discardOld = false)
    }

    override def postStop() {
      stat ! ServerEnd(self.path)
      log("Shuting down")
      log(viewsmap.foldLeft("") { (acc, n) =>
        acc + "\n" + n
      })

      // debugLog(s"view(${currentView.id},${currentView.leader},${currentView.participants.map(e => e.path.name).toList})")
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
        val numServers = (otherServers.size + 1) / 2
        if (numServers % 2 == 0) {
          this.quorumSize = (numServers) / replicationDegree + 1
        } else {
          this.quorumSize = (numServers) / replicationDegree
        }
        viewSetup()
      // paxos ! ServersConf(map)
      // electLeaderThenAnswer(sender)
      // context.unbecome()
      // coordinator ! Success("Ok " + self.path.name)
      // log("I'm ready")
      case UpdateView(keyHash, view) => {
        if (isLeader(keyHash)) {
          // println("someone just told a leader to update their view.")
        } else {
          //just update the view man
          viewsmap += (keyHash -> view)
          // println(s"$self updated its view.")
          if (currentView(keyHash).state.size > 0 && lastOperationId < currentView(keyHash).state.last.id) {
            // we have operations to perform
            // self ! currentView.state.last
          }
        }
      }
      case JoinView(keyHash, serverId, who) => {
        // possible cases
        if (this.serverId < serverId) {
          // only possible if I already own a quorum
          if (isLeader(keyHash)) {
            println(s"Received request to join the quorum.")
          } else {
            println("impossible!")
          }
        } else {
          numSlaves += 1
          if (!isLeader(keyHash) && numSlaves >= quorumSize) {
            // isLeader = true
            coordinator ! LeaderElected(self)
          }
        }
        // get new view ID
        // myCurrentViewId = currentView(keyHash).id + 1
        if (isLeader(keyHash)) {
          viewsmap += (keyHash -> View(currentView(keyHash).id, self, currentView(keyHash).participants ::: List(who), currentView(keyHash).state))
          // viewsmap += (keyHash -> View(currentView(keyHash).id, self, currentView(keyHash).participants, currentView(keyHash).state))
          println(s"${who.path.name} joined view of ${self.path.name}. Current leader is ${currentView(keyHash).leader.path.name}")
        } else {
          // viewsmap += (keyHash -> View(currentView(keyHash).id, currentView(keyHash).leader, currentView(keyHash).participants, currentView(keyHash).state))
          viewsmap += (keyHash -> View(currentView(keyHash).id, currentView(keyHash).leader, currentView(keyHash).participants ::: List(who), currentView(keyHash).state))
        }
        currentView(keyHash).participants.foreach(_ ! UpdateView(keyHash, currentView(keyHash))) // update view of all participants
      }
      case ServerDetails(keyHash, serverId, view) => {
        if (isLeader(keyHash)) {
          // quorum has already been formed by self, send details to sender
          sender ! ServerDetails(keyHash, this.serverId, currentView(keyHash))
        } else if (!isLeader(keyHash) && currentView(keyHash).participants.size >= quorumSize) {
          // self is already in a quorum, reply with the current view
          sender ! ServerDetails(keyHash, this.serverId, currentView(keyHash))
          println(s"${self.path.name} refused to join another view because it already is in a quorum.")
        } else if (!isLeader(keyHash) && currentView(keyHash).participants.size < quorumSize && view.participants.size < quorumSize) {
          // no quorum is known yet, decide using the process ID
          if (this.serverId > serverId) {
            sender ! ServerDetails(keyHash, this.serverId, currentView(keyHash))
          } else {
            view.leader ! JoinView(keyHash, this.serverId, self)
          }
        } else if (view.participants.size >= quorumSize) {
          // someone else already got a quorum, I'm going to join them
          view.leader ! JoinView(keyHash, this.serverId, self)
        } else {
          println("shit.")
        }
      }
      case _ => debugLog("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def isLeader(keyHash: String): Boolean = {
      var res = ((viewsmap.get(keyHash).map(_.leader == self).get)
        && ((viewsmap.getOrElse(keyHash, View(this.id, self, List(), List())).participants.size) >= quorumSize))
      debugLog("H: " + keyHash + " isLeader: " + res + " Q: " + quorumSize + " ALL: " + (viewsmap.getOrElse(keyHash, View(this.id, self, List(), List())).participants.size))
      res
    }

    def currentView(keyHash: String): View = {
      viewsmap.getOrElse(keyHash, View(this.id, self, List(), List()))
    }

    def viewSetup() = {
      var tmplist = 0 to (serversAddresses.size - 1)
      for (p <- 0 to ((tmplist.size / replicationDegree) - 1)) {
        var key = if (id - p >= 0) tmplist(id - p) else tmplist(tmplist.size + (id - p))
        var l = (key to (key + replicationDegree - 1)).map(e => e % tmplist.size).filter(e => e != id).map(e => serversAddresses.get("Server" + e)).flatMap(e => e).toList
        log(l.foldLeft(key"->") { (acc, n) =>
          acc + ", " + n.path.name
        })
        viewsmap += (key.toString -> new View(1, self, l, List()))
      }
      log(viewsmap.foldLeft("") { (acc, n) =>
        acc + "\n" + n
      })
      viewsmap.foreach(t => t._2.participants.foreach(e => e ! ServerDetails(t._1, this.id, t._2)))

      // viewsmap.foreach(e=> 
      //     otherServers.foreach(_ ! ServerDetails(serverId, currentView))

    }

    def receive(): Receive = {
      //case Stop =>
      //  context.stop(self)
      //case Alive =>
      //  debugLog("I'm alive")
      //  sender ! true
      //case WhoIsLeader =>
      //  heartbeatThenAnswer(sender)
      //case UpdateView(view) => {
      //  if (isLeader) {
      //    // println("someone just told a leader to update their view.")
      //  } else {
      //    //just update the view man
      //    currentView = view
      //    debugLog(s"Replica received view state:${view.state}")
      //    // println(s"$self updated its view.")
      //    if (currentView.state.size > 0 && lastOperationId < currentView.state.last.id) {
      //      val tmpList = (lastOperationId to currentView.state.last.id - 1).map(e => currentView.state(e)).toList
      //      for (op <- tmpList)
      //        op match {
      //          case Read(_, key) =>
      //            kvStore.get(key)
      //          case Write(_, key, value) =>
      //            kvStore += (key -> value)
      //        }
      //      lastOperationId = currentView.state.last.id
      //    }
      //  }
      //}

      //case JoinView(serverId, who) => {
      //  // possible cases
      //  if (this.serverId < serverId) {
      //    // only possible if I already own a quorum
      //    if (isLeader) {
      //      println(s"Received request to join the quorum.")
      //    } else {
      //      println("impossible!")
      //    }
      //  } else {
      //    numSlaves += 1
      //    if (!isLeader && numSlaves >= quorumSize) {
      //      isLeader = true
      //      coordinator ! LeaderElected(self)
      //    }
      //  }
      //  // we always update the view
      //  myCurrentViewId = 1 //TODO
      //  currentView = View(myCurrentViewId, self, currentView.participants ::: List(who), currentView.state)
      //  currentView.participants.foreach(_ ! UpdateView(currentView)) // send to all participants
      //  println(s"View increased size by 1. Current leader is ${currentView.leader.path.name}")
      //}
      ////
      //// client ops
      //case op: Action => {
      //  if (isLeader) {
      //    myCurrentViewId = 1 //TODO
      //    var newOp = op match {
      //      case Get(key) => { Read(getOperationId(), key) }
      //      case Put(key, value) => { Write(getOperationId(), key, value) }
      //    }
      //    currentView = View(myCurrentViewId, currentView.leader, currentView.participants, currentView.state ::: List(newOp))
      //    implicit val timeout = Timeout(MAX_EXEC_TIME)
      //    var clt = sender
      //    paxosViews ? Start(UpdateView(currentView)) onComplete {
      //      case Success(UpdateView(result)) =>
      //        debugLog("UpdateView by paxosViews: " + result)
      //        result.state.last match {
      //          case Read(_, key) => clt ! OperationSuccessful(s"Read($key)=${kvStore.get(key)}")
      //          case Write(_, key, value) =>
      //            kvStore += (key -> value)
      //            clt ! OperationSuccessful(s"Write($key,$value)")
      //        }
      //      case Success(result) => log("### Something went wrong ###ACTION: " + result)
      //      case Failure(failure) =>
      //        debugLog("UpdateView error by paxosViews: " + failure)
      //        clt ! OperationError(s"Op failed ")
      //    }
      //  }
      //}
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
          // isLeader = self == result
          // currentView = View(1, result, currentView.participants ::: otherServers.toList, currentView.state)
          if (alzheimer) //TODO CAREFULL
            actualLeader = None
          sender ! TheLeaderIs(result)
        case Success(result) => log("### Something went wrong: ### ELECTION " + result)
        case Failure(failure) =>
          // isLeader = false
          debugLog("Election failed: " + failure)
          actualLeader = None
      }
    }
  }
}
