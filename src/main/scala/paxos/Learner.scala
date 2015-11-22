package paxos

import akka.actor.{ ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString }
import akka.event.Logging
import scala.collection.mutable.MutableList

class Learner(ref: ActorRef, acceptorsSize: Int) extends Actor {

  val log = Logging(context.system, this)
  var quorum = 0
  var msgs = MutableList[Any]()
  var decided = false
  var debug = false

  def bota(text: String) = { if (debug) println(Console.RED + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

  def receive = {
    case Debug => debug = true
    case Learn(v) =>
      if (!decided) {
        quorum += 1
        msgs += v
        if (quorum > acceptorsSize / 2) {
          decided = true
          val value = msgs.groupBy(l => l).map(t => (t._1.toString, t._2.length)).toList.sortBy(_._2).max
          bota("Learner: RECV " + value._1)
          var actor = msgs.filter(e => e.toString == value._1).toList.head
          ref ! Learn(actor)
        }
      }
    case Stop =>
      quorum = 0
      decided = false
      sender ! Stop
      msgs = MutableList[Any]()
  }
}
