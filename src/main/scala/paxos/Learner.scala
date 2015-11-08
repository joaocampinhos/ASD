package paxos

import akka.actor.Actor
import akka.event.Logging

class Learner extends Actor {

  val log = Logging(context.system, this)

  var decided = false

  def bota(text: String) = { println(Console.RED+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE) }

  def receive = {
    case Decided(v) => {
      if (!decided) {
        decided = true
        bota("Decided("+v+")")
        System.exit(0)
      }
    }
  }
}
