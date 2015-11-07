package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Proposer(acceptors: Seq[ActorRef]) extends Actor {

  val log = Logging(context.system, this)

  //Valor a propor
  private val v = Random.nextInt(10)

  log.info("Proponho: "+v.toString)

  //Enviar para todos os acceptors
  def receive = {
    case "start" => acceptors.foreach(_ ! v)
  }

}


