package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Acceptor(learners: Seq[ActorRef]) extends Actor {

  val log = Logging(context.system, this)

  // O maior prepare ate agora
  var np:Option[Int] = None

  // Ultima proposta aceite
  var na:Option[Int] = None
  var va:Option[Int] = None

  def receive = {

    case Prepare(n) => {
      //log.info(n.toString)
      if(np.map(_ < n).getOrElse(true)) {
        np = Some(n)
        sender ! PrepareOk(na,va)
      }
      // TODO: ver se isto e mesmo assim
      /*
      else {
        sender ! PrepareAgain
      }
      */
    }

    case Accept(n, v) => {
      //log.info(n + " >= " +np)
      if(np.map(_ <= n).getOrElse(true)) {
        log.info("aceita isso!")
        na = Some(n)
        va = Some(v)
        sender ! AcceptOk(n)
      }
    }

    case Decided(v) => {
      learners.foreach(_ ! Decided(v))
    }

  }

}


