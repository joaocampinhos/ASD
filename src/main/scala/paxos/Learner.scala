package paxos

import akka.actor.ActorSelection
import akka.actor.{ ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString }
import akka.event.Logging
import scala.collection.mutable.MutableList

class Learner() extends Actor {
  var parents: Seq[ActorSelection] = Nil

  val log = Logging(context.system, this)

  var decided = false
  var quorum = 0
  var msgs = MutableList[Any]()

  def receive = {
    case Servers(servers) => parents = servers.toSeq

    case Learn(v:  Any) =>
      if (!decided) {
        quorum += 1
        msgs += v
        if (quorum >= calcQuorumDegree(parents.size)) {
          quorum = 0
          decided = true
          val value = msgs.groupBy(l => l).map(t => (t._1.toString, t._2.length)).toList.sortBy(_._2).max
          println("Q: " + calcQuorumDegree(parents.size) + "   Learner: RECV " + value._1)
          var actor = msgs.filter( e => e.toString == value._1).toList.head
          parents.foreach(e => e ! Learn(actor))
          msgs = MutableList[Any]()

          //System.exit(0)
        }
      }
    case Stop =>
      decided = false
      sender ! self.path
  }

  def calcQuorumDegree(value: Int): Int = {
    var res = Math.round(value / 2.0).toInt
    if (value % 2 == 0)
      res += 1
    res
  }
}
