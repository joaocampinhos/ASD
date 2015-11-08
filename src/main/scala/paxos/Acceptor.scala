package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Acceptor(learners: Seq[ActorRef]) extends Actor {

  val log = Logging(context.system, this)

  var decided = false;

  // O maior prepare ate agora
  var np:Option[Int] = None

  // Ultima proposta aceite
  var na:Option[Int] = None
  var va:Option[Int] = None

  def botap(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }
  def botaa(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.BLUE+text+Console.WHITE) }
  def botad(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE) }

  def receive = {

    case Prepare(n) => {
      if(np.map(_ < n).getOrElse(true)) {
        np = Some(n)
        botap("PrepareOk("+na+", "+va+")")
        sender ! PrepareOk(na,va)
      }
      // TODO: ver se isto e mesmo assim
      else {
        sender ! PrepareAgain
      }
    }

    case Accept(n, v) => {
      //log.info(n + " >= " +np)
      if(np.map(_ <= n).getOrElse(true)) {
        na = Some(n)
        va = Some(v)
        botaa("AcceptOk("+n+")")
        sender ! AcceptOk(n)
      }
    }

    case Decided(v) =>
      if (!decided) {
        decided = true;
        botad("Decided("+v+")")
        learners.foreach(_ ! Decided(v))
      }

  }

}


