package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Acceptor(learners: Seq[ActorRef]) extends Actor {

  val log = Logging(context.system, this)

  var done = false

  //Receber propostas e aceitar a primeira
  //enviar para os learners
  def receive = {
    case v:Int =>
      if (!done) {
        done = true
        log.info("Aceitamos: "+v.toString)
        learners.foreach(_ ! v)
      }
  }

}


