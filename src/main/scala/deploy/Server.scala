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
import stats.Stat.Messages.{ ServerStart, ServerEnd }
import views.Views.{ OperationSuccessful, OperationError, Write, Read, UpdateView, View, JoinView, LeaderElected }

object Server {
  val MAX_HEARTBEAT_TIME = 500.milliseconds
  val MAX_ELECTION_TIME = 500.milliseconds
  val MAX_EXEC_TIME = 1.seconds

  trait Action {
    def hash: Int
  }
  case class Get(hash: Int, key: String) extends Action
  case class Put(hash: Int, key: String, value: String) extends Action
  case object SearchTimeout
  case object Stop
  case class WhoIsLeader(hash: Int)
  case object Alive
  case class TheLeaderIs(l: Option[ActorRef])
  case class ServersConf(servers: collection.mutable.HashMap[String, ActorRef], paxoslist: List[ActorRef], paxosVList: List[ActorRef])

  case class ServerActor(id: Int, stat: ActorRef, replicationDegree: Int) extends Actor {
    import context.dispatcher
    val log = Logging(context.system, this)
    var debug = false
    var serversAddresses = HashMap[String, ActorRef]()
    var paxoslist = List[ActorRef]()
    var paxosVList = List[ActorRef]()
    var coordinator: ActorRef = null
    var kvStore = collection.mutable.Map[String, String]()
    var viewsmap = HashMap[Int, View]()
    var nlearns = 0

    stat ! ServerStart(self.path)

    override def preStart(): Unit = {
      context.become(waitForData(), discardOld = false)
    }

    override def postStop() {
      stat ! ServerEnd(self.path)
      log("Shuting down\n" + viewMapToShortString())
    }

    def log(text: Any) = { println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }
    def debugLog(text: Any) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

    def waitForData(): Receive = {
      case Stop =>
        context.stop(self)
      case ServersConf(map,electionPaxosList,viewsPaxosList) =>
        serversAddresses = map
        paxoslist = electionPaxosList
        paxosVList = viewsPaxosList
        coordinator = sender
        viewSetup()
      case _ => debugLog("[Stage: Waiting for servers' address] Received unknown message.")
    }

    def viewSetup() = {
      var tmplist = 0 to (serversAddresses.size - 1)
      for (p <- 0 to (replicationDegree - 1)) {
        var key = if (id - p >= 0) tmplist(id - p) else tmplist(tmplist.size + (id - p)) // calc view members idxs
        var l = (key to (key + replicationDegree - 1)) // transform to a list of refs without this instance
          .map(e => e % tmplist.size)
          .filter(e => e != id)
          .map(e => serversAddresses.get("Server" + e))
          .flatMap(e => e).toList
        viewsmap += (key -> new View(1, self, l, List()))
      }
      debugLog(viewMapToShortString())
      context.become(learn(), discardOld = true)
      viewsmap.foreach(t => { //Exec paxos so that every member of a view agrees on a leader
        paxoslist(t._1) ! Start(t._2)
      })
    }

    def learn(): Receive = {
      case (k: Int, v: View) =>
        nlearns += 1
        viewsmap += (k -> v)
        if (nlearns == replicationDegree) {
          log(viewMapToShortString())
          coordinator ! Alive
          context.unbecome()
        }
      case a: Any => log(a)
    }

    def receive(): Receive = {
      case Stop => context.stop(self)
      // case Alive =>
      //   debugLog("I'm alive")
      //   sender ! true
      case WhoIsLeader(hash) => {
        var view = consultView(hash)
        if (view.id > -1)
          sender ! TheLeaderIs(Some(consultView(hash).leader))
        else
          sender ! TheLeaderIs(None)
      }
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
            debugLog((tmpList.foldLeft("Executed Ops: ") { (acc, n) => acc + ", " + n }) + "\nUpdated to:" + viewMapToShortString())
          }
        }
      }
      case op: Action => {
        if (isLeader(op.hash)) {
          var currentView: View = consultView(op.hash)
          var newView =
            View(currentView.id + 1, currentView.leader, currentView.participants,
              currentView.state ::: List(op match {
                case Get(hash, key) => { Read(currentView.state.size, hash, key) }
                case Put(hash, key, value) => { Write(currentView.state.size, hash, key, value) }
              }))
          viewsmap += (op.hash -> newView)

          implicit val timeout = Timeout(MAX_EXEC_TIME)
          var clt = sender
          paxosVList(op.hash) ? Start(UpdateView(op.hash, newView)) onComplete {
            case Success(UpdateView(key, result)) =>
              debugLog("UpdateView by paxosViews: " + result)
              debugLog("Received: " + op + "\n" + "Updated View State: " + viewMapToShortString())
              result.state.last match {
                case Read(_, hash, key) => clt ! OperationSuccessful(s"Read($key)=${kvStore.get(key)}")
                case Write(_, hash, key, value) =>
                  kvStore += (key -> value)
                  clt ! OperationSuccessful(s"Write($key,$value)")
              }
            case Success(result) => debugLog("### Something went wrong ###ACTION: " + result)
            case Failure(failure) =>
              debugLog("UpdateView error by paxosViews: " + failure)
              clt ! OperationError(s"Op failed ")
          }
        }
      }
      case _ => debugLog("[Stage: Responding to Get/Put] Received unknown message.")
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

    def actorsToList(prefix: String, list: List[ActorRef]): String = {
      list.foldLeft(prefix) { (acc, n) => acc + ", " + n.path.name }
    }

    def viewMapToShortString(): String = {
      viewsmap.foldLeft("") { (acc, n) =>
        acc + "\nID: " + n._2.id + " L: " + n._2.leader.path.name + " K: " + actorsToList(n._1 + "->", n._2.participants) + " N_Ops:" + n._2.state.size
      }
    }

    def viewMapToString(): String = {
      viewsmap.foldLeft("") { (acc, n) =>
        acc + "\nID: " + n._2.id + " L: " + n._2.leader.path.name + " K: " + actorsToList(n._1 + "->", n._2.participants) + (n._2.state.foldLeft(" Ops:") { (acc2, n2) => acc2 + ", " + n2 })
      }
    }
  }
}
