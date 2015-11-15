package paxos

import akka.actor.ActorSelection
import akka.actor.{ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString}
import akka.event.Logging

class Learner() extends Actor {
  var parents: Seq[ActorSelection] = Nil

  val log = Logging(context.system, this)

  var decided = false

  def receive = {
    case Servers(servers) => parents = servers.toSeq

    case Learn(v) =>
      if (!decided) {
        decided = true
        parents.foreach(e =>  e ! Learn(v))
        //System.exit(0)
      }
    case Stop =>
      decided = false
      sender ! self.path
  }
}
