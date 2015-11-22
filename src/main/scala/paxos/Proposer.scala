package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Proposer extends Actor {

  val log = Logging(context.system, this)
  var debug = false

  //Nossa tag
  var nn: Int = 1

  //Valor inicial a propor
  var v: Any = Nil

  var acceptors: Seq[ActorRef] = Nil

  var oks: Seq[Option[Proposal]] = Nil
  var noks: Seq[Option[Proposal]] = Nil

  var quorum = 0

  def botap(text: String) = { if (debug) println(Console.CYAN + "[" + self.path.name + "] " + Console.YELLOW + text + Console.WHITE) }
  def botaa(text: String) = { if (debug) println(Console.CYAN + "[" + self.path.name + "] " + Console.BLUE + text + Console.WHITE) }
  def botad(text: String) = { if (debug) println(Console.CYAN + "[" + self.path.name + "] " + Console.GREEN + text + Console.WHITE) }

  def paxos(): Receive = {

    //turn on debug messages
    case Debug => debug = true

    case Start(value) =>
      v = value
      println("received my value")
    // Enviar Prepare
    case Go =>
      botap("SEND Prepare(" + nn + ")")
      acceptors.foreach(_ ! Prepare(nn))

    // Esperar pelo PrepareOk(na, va)
    case PrepareOk(prop) =>
      botap("RECV PrepareOk(" + prop + ")")
      oks = prop +: oks

      //Quorum
      if (oks.size > acceptors.size / 2) {
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        val value = oks.filter(_ != None)
          .sortBy {
            case None => -1
            case Some(Proposal(pN, _)) => pN
          }
          .headOption
          .getOrElse(Some(Proposal(nn, v)))
          .get
        botaa("SEND Accept(" + value + ")")
        acceptors.foreach(_ ! Accept(value))
        oks = Nil
      }

    //Caso de termos de enviar um novo prepare com um novo n
    case PrepareAgain(n) =>
      botap("RECV PrepareAgain(" + n + ")")
      quorum = quorum + 1
      if (quorum > acceptors.size / 2) {
        nn = n.getOrElse(nn) + 1
        botap("SEND Prepare(" + nn + ")")
        acceptors.foreach(_ ! Prepare(nn))
        quorum = 0
      }

    //Caso do accept falhar
    case AcceptAgain(prop) =>
      botaa("RECV AcceptAgain(" + prop + ")")
      noks = prop +: noks

      //Quorum
      if (noks.size > acceptors.size / 2) {
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        val value = noks.filter(_ != None)
          .sortBy {
            case None => -1
            case Some(Proposal(pN, _)) => pN
          }
          .headOption
          .getOrElse(Some(Proposal(nn, v)))
          .get
        botaa("SEND Accept(" + value + ")")
        acceptors.foreach(_ ! Accept(value))
        noks = Nil
      }

    //Caso nosso valor seja aceite
    case AcceptOk(n) => {
      //context.unbecome()
    }

    case Stop => {
      nn = 1
      v = Nil
      oks = Nil
      noks = Nil
      quorum = 0
      sender ! Stop
      context.unbecome()
    }

  }

  def receive = {

    case Servers(servers) => acceptors = servers

    case Operation(op) =>
      v = op
      context.become(paxos(), discardOld = false)

  }
}
