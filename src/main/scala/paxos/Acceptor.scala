package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Acceptor extends Actor {

  val log = Logging(context.system, this)
  var debug = false

  var learners: Seq[ActorRef] = Nil

  // O maior prepare ate agora
  var np:Option[Int] = None

  // Ultima proposta aceite
  var last:Option[Proposal] = None

  def botap(text: String) = { if (debug) println(Console.MAGENTA+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }
  def botaa(text: String) = { if (debug) println(Console.MAGENTA+"["+self.path.name+"] "+Console.BLUE+text+Console.WHITE)   }
  def botad(text: String) = { if (debug) println(Console.MAGENTA+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE)  }

  def receive = {

    //turn on debug messages
    case Debug => debug = true

    case Servers(servers) => learners = servers

    case Prepare(n) =>
      botap("RECV Prepare("+n+")")
        if(np.map(_ < n).getOrElse(true)) {
          np = Some(n)
          // botap("SEND PrepareOk("+last+")")
          sender ! PrepareOk(last)
        }
        else {
          // botap("SEND PrepareAgain("+last+")")
          sender ! PrepareAgain(np)
        }

    case Accept(prop) =>
      botaa("RECV Accept("+prop+")")
        if(np.map(_ <= prop.n).getOrElse(true)) {
          last = Some(prop)
          // botaa("SEND AcceptOk("+prop.n+")")
          sender ! AcceptOk(prop.n)
          // botad("SEND Learn("+prop.v+")")
          learners.foreach(_ ! Learn(prop.v))
        }
        else {
          // botaa("SEND AcceptAgain("+last+")")
          sender ! AcceptAgain(last)
        }

    case Stop => {
      np       = None
      last     = None
      sender ! Stop
    }

  }
}
