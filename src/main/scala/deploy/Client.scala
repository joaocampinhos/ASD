package deploy

import org.apache.commons.math3.distribution.ZipfDistribution
import scala.collection.mutable._
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import scala.concurrent.duration._
import com.typesafe.config.Config
import scala.concurrent.forkjoin.ThreadLocalRandom

case object DoRequest

object Client {
  case class ClientConf(readsPer: Int, maxOpsNumber: Int, zipfNumber: Int)

  def parseClientConf(config: Config) : ClientConf = {
    new ClientConf(
      config.getInt("ratioOfReads"),
      config.getInt("maxOpsPerClient"),
      config.getInt("numberOfZipfKeys")
    )
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

