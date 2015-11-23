package deploy

import akka.actor.Cancellable
import org.apache.commons.math3.distribution.ZipfDistribution
import collection.immutable.HashMap
import scala.collection.mutable.MutableList
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import scala.concurrent.duration._
import com.typesafe.config.Config
import scala.concurrent.forkjoin.ThreadLocalRandom
import org.apache.commons.math3.distribution.ZipfDistribution
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import paxos._

object Client {

  case class ClientConf(readsRate: Int, maxOpsNumber: Int, zipfNumber: Int)

  def parseClientConf(config: Config): ClientConf = {
    new ClientConf(
      config.getInt("ratioOfReads"),
      config.getInt("maxOpsPerClient"),
      config.getInt("numberOfZipfKeys")
    )
  }

  class ClientActor(serversURI: HashMap[String, ActorRef], clientConf: ClientConf) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    def scheduler = context.system.scheduler
    val zipf = new ZipfDistribution(clientConf.zipfNumber, 1)
    val rnd = ThreadLocalRandom.current
    var serverLeader: Option[ActorRef] = None
    var leaderQuorum = new MutableList[ActorRef]()
    var opsCounter = 0
    var alzheimer = true
    var debug = true
    var timeoutScheduler: Option[Cancellable] = None
    scheduler.scheduleOnce(1.seconds, self, DoRequest)

    bota("Started")

    def bota(text: String) = { if (debug) println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE) }

    def receive = {
      case DoRequest =>
        var op = createOperation()
        bota("Prepared " + op)
        sendToLeader(op, false)
      case a: Any => bota("[Stage:Getting Leader Address] Received unknown message. " + a)
    }

    def waitingForLeaderInfo(op: Action): Receive = {
      case "timeout" =>
        bota("Timeout")
        findLeader(op, true)
      case TheLeaderIs(l) => {
        leaderQuorum += l
        if (leaderQuorum.size > serversURI.size / 2) {
          val leaderAddress = leaderQuorum.groupBy(l => l).map(t => (t._1, t._2.length)).toList.sortBy(_._2).max
          if (leaderAddress._2 > serversURI.size / 2) {
            serverLeader = Some(leaderAddress._1)
            sendToLeader(op, true)
          } else {
            bota("Retry")
            findLeader(op, true)
          }
        }
      }
      case a: Any => bota("[Stage:Getting Leader Address] Received unknown message. " + a)
    }

    def sendToLeader(op: Action, consecutiveError: Boolean) = {
      context.unbecome()
      serverLeader match {
        case Some(l) => {
          bota("leader is " + l)
          implicit val timeout = Timeout(5.seconds)
          l ? op onComplete {
            case Success(result) =>
              bota("Op result: " + result)
              resetRole()
            case Failure(failure) =>
              bota("Op failed: " + failure)
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
      bota("Finding leader")
      leaderQuorum = new MutableList[ActorRef]()
      serversURI.values.foreach(e => e ! WhoIsLeader)
      context.become(waitingForLeaderInfo(op), discardOld = consecutiveError)
      cancelTimer()
      timeoutScheduler = Some(scheduler.scheduleOnce(1.seconds, self, "timeout"))
    }

    def createOperation(): Action = {
      opsCounter += 1
      bota("N_OP:" + opsCounter)
      if (rnd.nextInt(0, 101) <= clientConf.readsRate)
        Get(zipf.sample().toString)
      else {
        Put(zipf.sample().toString, zipf.sample().toString)
      }
    }

    def resetRole() = {
      opsCounter match {
        case clientConf.maxOpsNumber =>
          bota("Executed all ops")
          context.stop(self) // Client has executed all operations
        case _ => {
          context.unbecome()
          scheduler.scheduleOnce(0.seconds, self, DoRequest)
        }
      }
    }
    def cancelTimer() = {
      timeoutScheduler match {
        case Some(t) => t.cancel()
        case None => Unit
      }
    }
  }
}
