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
    println(serversURI.size)

    val log = Logging(context.system, this)
    def scheduler = context.system.scheduler
    val zipf = new ZipfDistribution(clientConf.zipfNumber, 1)
    val rnd = ThreadLocalRandom.current
    var lastOpWasRead: Option[Boolean] = None
    var serverLeader: Option[ActorRef] = Some(serversURI.values.head)
    var leaderQuorum = new MutableList[ActorRef]()
    var opsCounter = 0
    scheduler.scheduleOnce(1.seconds, self, DoRequest)

    bota("Started")

    def bota(text: String) = { println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE) }

    def receive = {
      case DoRequest =>
        var op = createOperation()
        bota("Prepared " + op)
        sendToLeader(op, false)
      case _ => bota("[Stage: Operation's preparation] Received unknown message.")
    }

    def findLeader(op: Operation): Receive = { //TODO Exclude the case where every server has one vote
      case TheLeaderIs(l) => {
        leaderQuorum += l
        if (leaderQuorum.size == serversURI.size) {
          // if (leaderQuorum.size == calcQuorumDegree(serversURI.size)) { //TODO the above line by this one
          val leaderAddress = leaderQuorum.groupBy(l => l).map(t => (t._1, t._2.length)).toList.sortBy(_._2).max
          if (leaderAddress._2 > 1) {
            leaderQuorum = new MutableList[ActorRef]()
            sendToLeader(op, true)
            resetRole()
          } else {
            bota("[Stage: Getting Leader Address] No leader was chosen")
            resetRole()
          }
        }
      }
      case _ => bota("[Stage:Getting Leader Address] Received unknown message.")
    }

    def sendToLeader(op: Operation, sucessiveError: Boolean) = {
      serverLeader match {
        case Some(l) => {
          implicit val timeout = Timeout(5.seconds)
          l ? op onComplete {
            case Success(result) =>
              bota("Sending " + op + " to " + l.path.name)
              println(result)
              scheduler.scheduleOnce(0.seconds, self, DoRequest)
              if (sucessiveError)
                context.unbecome()
            case Failure(failure) =>
              println(failure)
              sendToAll(op, sucessiveError)
          }
        }
        case None => sendToAll(op, sucessiveError)
      }
    }

    def sendToAll(op: Operation, sucessiveError: Boolean) = {
      serversURI.values.foreach(e => e ! WhoIsLeader)
      context.become(findLeader(op), discardOld = sucessiveError)
    }

    def calcQuorumDegree(value: Int): Int = {
      var res = Math.round(value / 2.0).toInt
      if (value % 2 == 0 && value > 2)
        res += 1
      res
    }

    def createOperation(): Operation = {
      if (opsCounter == clientConf.maxOpsNumber) {
        context.stop(self)
      }
      val isOpRead = rnd.nextInt(0, 101) <= clientConf.readsRate
      lastOpWasRead = Some(isOpRead)
      if (isOpRead)
        Get(zipf.sample().toString)
      else {
        Put(zipf.sample().toString, zipf.sample().toString)
      }
    }

    def resetRole() = {
      scheduler.scheduleOnce(0.seconds, self, DoRequest)
      context.unbecome()
    }

  }
}
