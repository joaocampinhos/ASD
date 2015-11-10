package deploy

import org.apache.commons.math3.distribution.ZipfDistribution
import scala.collection.mutable._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorPath
import akka.event.Logging
import akka.actor.ActorSelection
import akka.actor.ActorRef
import scala.concurrent.duration._
import com.typesafe.config.Config
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.Cancellable
import akka.actor.AddressFromURIString
import akka.actor.{ Address, AddressFromURIString }

case object DoRequest

object Client {
  case class ClientConf(replicationDegree: Int, quorumDegree: Int, readsPer: Int, maxOpsNumber: Int, zipfNumber: Int)

  def parseClientConf(config: Config) : ClientConf = {
    new ClientConf(
      config.getInt("replicationDegree"),
      config.getInt("ratioOfReads"),
      config.getInt("maxOpsPerClient"),
      config.getInt("numberOfZipfKeys"),
      calcQuorumDegree(config.getInt("replicationDegree")))
  }

  def calcQuorumDegree(value: Int) : Int = {
    var res= Math.round(value / 2.0).toInt
    if(value % 2 == 0 && value > 2)
      res+=1
    res
  }

  class ClientActor(serversURI:HashMap[String,ActorRef],clientConf: ClientConf) extends Actor {
    import context.dispatcher

    val log = Logging(context.system, this)
    def scheduler = context.system.scheduler
    scheduler.scheduleOnce(1.seconds,self,DoRequest)
    bota("Started")

    def bota(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }

    def receive = {
      case DoRequest => doSomething()
      case _ => log.info("Received unknown message on phase")
    }

    def doSomething() = {
    }
  }

}

