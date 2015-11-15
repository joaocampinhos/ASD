package deploy

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

abstract class Operation
case class Get(key: String) extends Operation
case class Put(key: String, value: String) extends Operation
case object WhoIsLeader
case class TheLeaderIs(l: ActorRef)
case object DoRequest
case object Alive

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
    var lastOpWasRead: Option[Boolean] = None
    var serverLeader: Option[ActorRef] = None
    var leaderQuorum = new MutableList[ActorRef]()
    var opsCounter = 0
    var alzheimer = true
    var debug  = true
    scheduler.scheduleOnce(1.seconds, self, DoRequest)

    bota("Started")

    def bota(text: String) = {if (debug)  println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE) }

    def receive = {
      case DoRequest =>
        var op = createOperation()
        bota("Prepared " + op)
        sendToLeader(op, false)
      case _ => bota("[Stage: Operation's preparation] Received unknown message.")
    }

    def findLeader(op: Operation): Receive = {
      case TheLeaderIs(l) => {
        leaderQuorum += l
        if (leaderQuorum.size == serversURI.size) {
          val leaderAddress = leaderQuorum.groupBy(l => l).map(t => (t._1, t._2.length)).toList.sortBy(_._2).max
          if (leaderAddress._2 == serversURI.size) {
            serverLeader = Some(leaderAddress._1)
            leaderQuorum = new MutableList[ActorRef]()
            sendToLeader(op, true)
          } else {
            bota("Cluster has no leader")
            context.stop(self)
          }
        }
      }
      case _ => bota("[Stage:Getting Leader Address] Received unknown message.")
    }

    def sendToLeader(op: Operation, consecutiveError: Boolean) = {
      serverLeader match {
        case Some(l) => {
          implicit val timeout = Timeout(5.seconds)
          l ? op onComplete {
            case Success(result) =>
              bota("Sent " + result + " to " + l.path.name)
              resetRole()
            case Failure(failure) =>
              bota("Failed to send " + op + " to " + l.path.name + ".Reason: " + failure)
              serverLeader = None
              sendToAll(op, consecutiveError)
          }
        }
        case None =>
          sendToAll(op, consecutiveError)

      }
      if(alzheimer)
        serverLeader = None //REMOVE ON FINAL STAGE
    }

    def sendToAll(op: Operation, consecutiveError: Boolean) = {
      bota("Finding leader")
      serversURI.values.foreach(e => e ! WhoIsLeader)
      context.become(findLeader(op), discardOld = consecutiveError)
    }

    def createOperation(): Operation = {
      val isOpRead = rnd.nextInt(0, 101) <= clientConf.readsRate
      lastOpWasRead = Some(isOpRead)
      opsCounter += 1
      bota("N_OP:" + opsCounter)
      if (isOpRead)
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
          scheduler.scheduleOnce(0.seconds, self, DoRequest)
          context.unbecome()
        }
      }
    }
  }
}
