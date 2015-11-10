package deploy

import scala.collection.mutable.Set
import scala.collection.immutable._
import akka.actor.Props
import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.math.Ordering.Implicits._

class Server() extends Actor {
  import context.dispatcher
  val log = Logging(context.system, this)
  var store = scala.collection.mutable.HashMap[String,String]()
  bota("Started")

  def bota(text: String) = { println(Console.RED+"["+self.path+"] "+Console.GREEN+text+Console.WHITE) }

  def receive = {
    case _ => log.info("Received unknown message")
  }
}
