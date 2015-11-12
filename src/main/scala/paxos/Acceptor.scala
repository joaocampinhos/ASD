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
  var last:Option[Proposal] = None

  def botap(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }
  def botaa(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.BLUE+text+Console.WHITE) }
  def botad(text: String) = { println(Console.MAGENTA+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE) }

  def receive = {

    case Prepare(n) =>
      if (!decided) {
        if(np.map(_ < n).getOrElse(true)) {
          np = Some(n)
          //botap("PrepareOk("+na+", "+va+")")
          sender ! PrepareOk(last)
        }
        else {
          //botap("PrepareAgain("+na+", "+va+")")
          sender ! PrepareAgain(np)
        }
      }

    case Accept(prop) =>
      if (!decided) {
        if(np.map(_ <= prop.n).getOrElse(true)) {
          decided = true;
          last = Some(prop)
          sender ! AcceptOk(prop.n)
          learners.foreach(_ ! Learn(prop.v))
        }
        else {
          //botaa("AcceptAgain("+na+", "+va+")")
          println(last toString)
          sender ! AcceptAgain(last)
        }
      }

  }
}

