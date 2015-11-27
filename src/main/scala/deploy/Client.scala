package deploy

import org.apache.commons.math3.distribution.ZipfDistribution
import org.apache.commons.math3.distribution.ZipfDistribution
import com.typesafe.config.Config
import akka.actor.Cancellable
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.{ Actor, ActorRef }
import akka.event.Logging
import collection.mutable.{ HashMap, MutableList }
import concurrent.duration._
import concurrent.forkjoin.ThreadLocalRandom
import concurrent.Await
import util.{ Failure, Success }
import deploy.Server.{ Get, Put, Action, WhoIsLeader, TheLeaderIs, Alive }
import stats.Stat.Messages.{ ClientStart, ClientEnd, StatOp, Lat }
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import paxos._

object Client {
  val DO_OP_TIME = 0.seconds
  val ANSWER_TIME = 1.seconds
  val LEADER_ANSWER_TIME = 1.seconds
  case class ClientConf(readsRate: Int, maxOpsNumber: Int, zipfNumber: Int)
  case object DoRequest
  case object IsAlive

  def parseClientConf(config: Config): ClientConf = {
    new ClientConf(
      config.getInt("ratioOfReads"),
      config.getInt("maxOpsPerClient"),
      config.getInt("numberOfZipfKeys")
    )
  }

  case class ClientActor(serversURI: HashMap[String, ActorRef], clientConf: ClientConf, stat: ActorRef, replicationDegree: Int) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    def scheduler = context.system.scheduler
    val zipf = new ZipfDistribution(clientConf.zipfNumber, 1)
    val rnd = ThreadLocalRandom.current
    var serverLeader: Option[ActorRef] = None
    var idxMap = HashMap[Int, ActorRef]()
    var leaderQuorum = new MutableList[Option[ActorRef]]()
    var opsCounter = 0
    var alzheimer = false
    var debug = false
    var timeoutScheduler: Option[Cancellable] = None

    var start: Long = 0
    var end: Long = 0
    var sum: Long = 0

    log("I'm ready")
    stat ! ClientStart(self.path)

    def log(text: Any) = {
      println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE)
    }

    def debugLog(text: Any) = {
      if (debug) println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE)
    }

    override def postStop() {
      stat ! ClientEnd(self.path)
    }

    def receive = {
      case IsAlive => sender ! Alive
      case DoRequest =>
        var op = createOperation()
        op match {
          case Get(_, _) => stat ! StatOp(self.path, "get")
          case Put(_, _, _) => stat ! StatOp(self.path, "put")
        }
        start = java.lang.System.currentTimeMillis()
        sendToLeader(op, false)
      case a: Any => debugLog("[Stage:Getting Leader Address] Received unknown message. " + a)
    }
    def waitingForLeaderInfo(op: Action): Receive = {
      case Timeout =>
        debugLog("Timeout")
        findLeader(op, true)
      case TheLeaderIs(l) => {
        leaderQuorum += l
        if (leaderQuorum.flatMap(e => e).size > replicationDegree / 2) {
          val leaderAddress = leaderQuorum.flatMap(e => e).groupBy(l => l).map(t => (t._1, t._2.length)).toList.sortBy(_._2).max
          if (leaderAddress._2 > replicationDegree / 2) {
            idxMap += (op.hash -> leaderAddress._1)
            sendToLeader(op, true)
          } else {
            debugLog("Retry")
            findLeader(op, true)
          }
        }
      }
      case a: Any => debugLog("[Stage:Getting Leader Address] Received unknown message. " + a)
    }

    def sendToLeader(op: Action, consecutiveError: Boolean) = {
      context.unbecome()
      idxMap.get(op.hash) match {
        case Some(actor) =>
          debugLog("hash -> " + actor)
          implicit val timeout = Timeout(LEADER_ANSWER_TIME)
          actor ? op onComplete {
            case Success(result) =>
              end = java.lang.System.currentTimeMillis()
              sum += (end - start)
              log(result + " => OP:" + op + " on " + actor.path.name)
              resetRole()
            case Failure(failure) =>
              end = java.lang.System.currentTimeMillis()
              sum += (end - start)
              log("OP:" + op + " failed: " + failure + " on " + actor.path.name)
              debugLog("Total servers " + serversURI.size)
              idxMap -= op.hash
              findLeader(op, consecutiveError)
          }
        case None =>
          debugLog("I have no record for such key:" + op.hash)
          findLeader(op, consecutiveError) //TODO
      }
    }

    def findLeader(op: Action, consecutiveError: Boolean) = {
      debugLog("Finding leader for key: " + op.hash)
      leaderQuorum = new MutableList[Option[ActorRef]]()
      serversURI.values.foreach(e => e ! WhoIsLeader(op.hash))
      context.become(waitingForLeaderInfo(op), discardOld = consecutiveError)
      setNewFindTimeout()
    }

    def createOperation(): Action = {
      var op = if (rnd.nextInt(0, 101) <= clientConf.readsRate) {
        var str = zipf.sample().toString
        Get(calcHash(str), str)
      } else {
        var str = (zipf.sample().toString, zipf.sample().toString)
        Put(calcHash(str._1), str._1, str._2)
      }
      opsCounter += 1
      debugLog("Nº:" + opsCounter + " OP: " + op)
      op
    }

    def calcHash(str: String): Int = {
      str.hashCode() % serversURI.size
    }

    def resetRole() = {
      opsCounter match {
        case clientConf.maxOpsNumber =>
          stat ! Lat(self.path, sum.toInt)
          log("Executed all ops")
          cancelOpTimeout()
          //Tempo total
          context.stop(self) // Client has executed all operations
        case _ => {
          if (alzheimer)
            idxMap = HashMap[Int, ActorRef]()
          context.unbecome()
          scheduler.scheduleOnce(DO_OP_TIME, self, DoRequest)
        }
      }
    }

    def setNewFindTimeout() = {
      cancelOpTimeout()
      timeoutScheduler = Some(scheduler.scheduleOnce(ANSWER_TIME, self, Timeout))
    }

    def cancelOpTimeout() = {
      timeoutScheduler match {
        case Some(t) => t.cancel()
        case None => Unit
      }
    }
  }
}
