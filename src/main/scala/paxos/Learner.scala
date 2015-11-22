package paxos

import akka.actor.{ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString}
import akka.event.Logging

class Learner(ref:ActorRef) extends Actor {

  val log = Logging(context.system, this)

  var decided = false

  def receive = {
    case Learn(v) =>
      if (!decided) {
        decided = true
        ref ! Learn(v)
        //System.exit(0)
      }
    case Stop =>
      decided = false
      sender ! Stop
  }
}
