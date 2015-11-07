package paxos

import akka.actor.Actor
import akka.event.Logging

class Learner extends Actor {

  val log = Logging(context.system, this)

  def receive = {
    case Decided(v) => {
      log.info("Decidimos: "+v.toString)
    }
  }
}
