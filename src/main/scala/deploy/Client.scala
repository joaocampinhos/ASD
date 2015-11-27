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
import stats.Messages.{ ClientStart, ClientEnd, StatOp }

object Client {
  val DO_OP_TIME = 2.seconds
  val ANSWER_TIME = 2.seconds
  val LEADER_ANSWER_TIME = 2.seconds
  case class ClientConf(readsRate: Int, maxOpsNumber: Int, zipfNumber: Int)
  case object DoRequest

  def parseClientConf(config: Config): ClientConf = {
    new ClientConf(
      config.getInt("ratioOfReads"),
      config.getInt("maxOpsPerClient"),
      config.getInt("numberOfZipfKeys")
    )
  }

  case class ClientActor(serversURI: HashMap[String, ActorRef], clientConf: ClientConf, stat: ActorRef) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    def scheduler = context.system.scheduler
    val zipf = new ZipfDistribution(clientConf.zipfNumber, 1)
    val rnd = ThreadLocalRandom.current
    var serverLeader: Option[ActorRef] = None
    var leaderQuorum = new MutableList[ActorRef]()
    var opsCounter = 0
    var alzheimer = false
    var debug = false
    var timeoutScheduler: Option[Cancellable] = None

    log("I'm ready")
    stat ! ClientStart(self.path)

    scheduler.scheduleOnce(DO_OP_TIME, self, DoRequest)

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
      case DoRequest =>
        var op = createOperation()
        op = Get(0,"ola")
        serversURI.values.foreach(e=> e ! op)
        // op match {
        //   case Get(_,_) => stat ! StatOp(self.path, "get")
        //   case Put(_,_, _) => stat ! StatOp(self.path, "put")
        // }
        // sendToLeader(op, false)
      case a: Any => debugLog("[Stage:Getting Leader Address] Received unknown message. " + a)
    }

    def waitingForLeaderInfo(op: Action): Receive = {
      case Timeout =>
        debugLog("Timeout")
        findLeader(op, true)
      case TheLeaderIs(l) => {
        leaderQuorum += l
        if (leaderQuorum.size > serversURI.size / 2) {
          val leaderAddress = leaderQuorum.groupBy(l => l).map(t => (t._1, t._2.length)).toList.sortBy(_._2).max
          if (leaderAddress._2 > serversURI.size / 2) {
            serverLeader = Some(leaderAddress._1)
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
      serverLeader match {
        case Some(l) => {
          debugLog("leader is " + l)
          implicit val timeout = Timeout(LEADER_ANSWER_TIME)
          l ? op onComplete {
            case Success(result) =>
              log(result + " => OP:" + op + " on " + l.path.name)
              op match {
                case Get(_,_) => stat ! StatOp(self.path, "getsuc")
                case Put(_, _,_) => stat ! StatOp(self.path, "putsuc")
              }
              resetRole()
            case Failure(failure) =>
              log("OP:" + op + " failed: " + failure + " on " + l.path.name)
              serversURI -= l.path.name
              debugLog("Total servers " + serversURI.size)
              serverLeader = None
              findLeader(op, consecutiveError)
          }
        }
        case None =>
          findLeader(op, consecutiveError)
      }
      if (alzheimer)
        serverLeader = None //REMOVE ON FINAL STAGE
    }

    def findLeader(op: Action, consecutiveError: Boolean) = {
      debugLog("Finding leader")
      leaderQuorum = new MutableList[ActorRef]()
      serversURI.values.foreach(e => e ! WhoIsLeader)
      context.become(waitingForLeaderInfo(op), discardOld = consecutiveError)
      cancelOpTimeout()
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

    def calcHash(str: String) :Int = {
        str.hashCode() % serversURI.size
        serverLeader= serversURI.get("Server0")
        0
    }

    def resetRole() = {
      opsCounter match {
        case clientConf.maxOpsNumber =>
          log("Executed all ops")
          cancelOpTimeout()
          context.stop(self) // Client has executed all operations
        case _ => {
          context.unbecome()
          scheduler.scheduleOnce(DO_OP_TIME, self, DoRequest)
        }
      }
    }

    def cancelOpTimeout() = {
      timeoutScheduler match {
        case Some(t) => t.cancel()
        case None => Unit
      }
      timeoutScheduler = Some(scheduler.scheduleOnce(ANSWER_TIME, self, Timeout))
    }
  }
}
