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

  trait Action {
    def hash: Int
  }
  case class Get(hash: Int, key: String) extends Action
  case class Put(hash: Int, key: String, value: String) extends Action
  case object SearchTimeout
  case object Stop
  case object WhoIsLeader
  case object Alive
  case class TheLeaderIs(l: ActorRef)
  case class ServersConf(servers: collection.mutable.HashMap[String, ActorRef])

  case class ServerActor(id: Int, paxos: ActorRef, paxosViews: ActorRef, stat: ActorRef, paxoslist: List[ActorRef], paxosVList: List[ActorRef]) extends Actor {
    import context.dispatcher
    var replicationDegree = 3
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

    var viewsmap = HashMap[Int, View]()
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
      //case JoinView(keyHash, serverId, who) => {
      //  // possible cases
      //  if (this.serverId < serverId) {
      //    // only possible if I already own a quorum
      //    if (isLeader(keyHash)) {
      //      println(s"Received request to join the quorum.")
      //    } else {
      //      println("impossible!")
      //    }
      //  } else {
      //    numSlaves += 1
      //    if (!isLeader(keyHash) && numSlaves >= quorumSize) {
      //      // isLeader = true
      //      coordinator ! LeaderElected(self)
      //    }
      //  }
      //  // get new view ID
      //  // myCurrentViewId = currentView(keyHash).id + 1
      //  if (isLeader(keyHash)) {
      //    viewsmap += (keyHash -> View(currentView(keyHash).id, self, currentView(keyHash).participants ::: List(who), currentView(keyHash).state))
      //    // viewsmap += (keyHash -> View(currentView(keyHash).id, self, currentView(keyHash).participants, currentView(keyHash).state))
      //    println(s"${who.path.name} joined view of ${self.path.name}. Current leader is ${currentView(keyHash).leader.path.name}")
      //  } else {
      //    // viewsmap += (keyHash -> View(currentView(keyHash).id, currentView(keyHash).leader, currentView(keyHash).participants, currentView(keyHash).state))
      //    viewsmap += (keyHash -> View(currentView(keyHash).id, currentView(keyHash).leader, currentView(keyHash).participants ::: List(who), currentView(keyHash).state))
      //  }
      //  currentView(keyHash).participants.foreach(_ ! UpdateView(keyHash, currentView(keyHash))) // update view of all participants
      //}
      case _ => debugLog("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def isLeader(keyHash: Int): Boolean = {
      viewsmap.get(keyHash) match {
        case Some(v) =>
          debugLog("H: " + keyHash + " isLeader: " + (v.leader == self))
          v.leader == self
        case None => false
      }
    }

    def consultView(keyHash: Int): View = {
      viewsmap.getOrElse(keyHash, View(-1, self, List(), List()))
    }

    def viewSetup() = {
      var tmplist = 0 to (serversAddresses.size - 1)
      for (p <- 0 to (replicationDegree - 1)) {
        var key = if (id - p >= 0) tmplist(id - p) else tmplist(tmplist.size + (id - p))
        var l = (key to (key + replicationDegree - 1)).map(e => e % tmplist.size).filter(e => e != id).map(e => serversAddresses.get("Server" + e)).flatMap(e => e).toList
        viewsmap += (key -> new View(1, self, l, List()))
      }
      debugLog(viewMapToString())
      context.become(learn(), discardOld = true)
      viewsmap.foreach(t => {
        paxoslist(t._1) ! Start(t._2)
      })
    }

    def viewMapToString(): String = {
      viewsmap.foldLeft("") { (acc, n) =>
        acc + "\n" + "L: " + n._2.leader.path.name + " K: " + printlist(n._1 + "->", n._2.participants) + (n._2.state.foldLeft(" Ops:") { (acc2, n2) => acc2 + ", " + n2 })
      }
    }
    var nlearns = 0
    def learn(): Receive = {
      case (k: Int, v: View) =>
        nlearns += 1
        viewsmap += (k -> v)
        if (nlearns == replicationDegree) {
          log(viewMapToString())
          context.unbecome()
        }
      case a: Any =>
        log(a)
    }

    def printlist(prefix: String, list: List[ActorRef]): String = {
      list.foldLeft(prefix) { (acc, n) => acc + ", " + n.path.name }
    }

    def receive(): Receive = {
      case Stop =>
        context.stop(self)
      // case Alive =>
      //   debugLog("I'm alive")
      //   sender ! true
      // case WhoIsLeader =>
      //   heartbeatThenAnswer(sender)

      case UpdateView(keyHash, view) => {
        if (!isLeader(keyHash) && consultView(keyHash).id != -1) {
          if (view.state.size > 0 && consultView(keyHash).state.size < view.state.size) {
            val tmpList = (consultView(keyHash).state.size to view.state.size - 1).map(e => view.state(e)).toList
            for (op <- tmpList)
              op match {
                case Read(_, hash, key) =>
                  kvStore.get(key)
                case Write(_, hash, key, value) =>
                  kvStore += (key -> value)
              }
            viewsmap += (keyHash -> view)
            log((tmpList.foldLeft("Executed Ops: ") { (acc, n) => acc + ", " + n })+"\nUpdated to:" + viewMapToString())
          }
        }
      }

      case op: Action => {
        if (isLeader(op.hash)) {
          var newOp = op match {
            case Get(hash, key) => { Read(getOperationId(), hash, key) }
            case Put(hash, key, value) => { Write(getOperationId(), hash, key, value) }
          }

          var currentView: View = consultView(newOp.hash)
          var newView = View(currentView.id, currentView.leader, currentView.participants, currentView.state ::: List(newOp))
          viewsmap += (newOp.hash -> newView)

          implicit val timeout = Timeout(MAX_EXEC_TIME)
          var clt = sender
          paxosVList(op.hash) ? Start(UpdateView(newOp.hash, newView)) onComplete {
            case Success(UpdateView(key, result)) =>
              debugLog("UpdateView by paxosViews: " + result)
              log("Received: " + op + "\n" + "Updated View State: " + viewMapToString())
              result.state.last match {
                case Read(_, hash, key) => clt ! OperationSuccessful(s"Read($key)=${kvStore.get(key)}")
                case Write(_, hash, key, value) =>
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
