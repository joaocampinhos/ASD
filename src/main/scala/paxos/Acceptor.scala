package paxos

import akka.actor._
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Acceptor extends Actor {

  val log = Logging(context.system, this)
  var debug = true

  var learners: Seq[ActorSelection] = Nil

  // O maior prepare ate agora
  var np: Option[Int] = None

  // Ultima proposta aceite
  var last: Option[Proposal] = None

  def botap(text: String) = { if (debug) println(Console.MAGENTA + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE) }
  def botaa(text: String) = { if (debug) println(Console.MAGENTA + "[" + self.path.name + "] " + Console.BLUE + text + Console.WHITE) }
  def botad(text: String) = { if (debug) println(Console.MAGENTA + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

  def receive = {

    //turn on debug messages
    case Debug => debug = true

    case Servers(servers) => learners = servers.toSeq

    case Prepare(n) =>
      botap("RECV Prepare(" + n + ")")
      if (np.map(_ < n).getOrElse(true)) {
        np = Some(n)
        // botap("SEND PrepareOk(" + last + ")")
        sender ! PrepareOk(last)
      } else {
        // botap("SEND PrepareAgain(" + last + ")")
        sender ! PrepareAgain(np)
      }

    case Accept(prop) =>
      botaa("RECV Accept(" + prop + ")")
      println("Old value" + np)
      if (np.map(_ <= prop.n).getOrElse(true)) {
        last = Some(prop)
        // botaa("SEND AcceptOk(" + prop.n + ")")
        sender ! AcceptOk(prop.n)
        botad("SEND Learn(" + prop.v + ")")
        learners.foreach(_ ! Learn(prop.v))
      } else {
        // botaa("SEND AcceptAgain(" + last + ")")
        sender ! AcceptAgain(last)
      }

    case Stop => {
      np = None
      last = None
      sender ! Stop
    }

  }
}
