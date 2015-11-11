package paxos

import akka.actor.Actor
import akka.event.Logging

class Learner extends Actor {

  val log = Logging(context.system, this)

  var decided = false

  def receive = {
    case Learn(v) =>
      if (!decided) {
        decided = true
        log.info(v.toString)
        //System.exit(0)
      }
  }
}
