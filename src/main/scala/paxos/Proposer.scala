package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Proposer extends Actor {

  val log = Logging(context.system, this)

  //Nossa tag
  var nn:Int = 1

  //Valor inicial a propor
  var v:Any = Nil

  var acceptors: Seq[ActorRef] = Nil

  var oks: Seq[Option[Proposal]] = Nil
  var noks: Seq[Option[Proposal]] = Nil

  var quorum = 0

  def botap(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }
  def botaa(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.BLUE+text+Console.WHITE) }
  def botad(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE) }

  def paxos() : Receive = {

    // Enviar Prepare
    case Start =>
      botap("Prepare("+nn+")")
      acceptors.foreach(_ ! Prepare(nn))

    // Esperar pelo PrepareOk(na, va)
    case PrepareOk(prop) =>
      oks = prop +: oks

      //Quorum
      if (oks.size > acceptors.size / 2) {
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        acceptors.foreach(_ ! Accept(Proposal(nn, v)))
        val value = oks.filter(_ != None)
          .sortBy {
            case None => -1
            case Some(Proposal(pN, _)) => pN
          }
          .headOption
          .getOrElse(Some(Proposal(nn,v)))
          .get
        acceptors.foreach(_ ! Accept(value))
        botaa("Accept("+value+")")
        oks = Nil
      }

    //Caso de termos de enviar um novo prepare com um novo n
    case PrepareAgain(n) =>
      quorum = quorum + 1
      if (quorum > acceptors.size / 2) {
        nn = n.getOrElse(nn) + 1
        botap("Prepare("+nn+")")
        acceptors.foreach(_ ! Prepare(nn))
        quorum = 0
      }

    //Caso do accept falhar
    case AcceptAgain(prop) =>
      noks = prop +: noks

      //Quorum
      if (noks.size > acceptors.size / 2) {
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        acceptors.foreach(_ ! Accept(Proposal(nn, v)))
        val value = noks.filter(_ != None)
          .sortBy {
            case None => -1
            case Some(Proposal(pN, _)) => pN
          }
          .headOption
          .getOrElse(Some(Proposal(nn,v)))
          .get
        acceptors.foreach(_ ! Accept(value))
        botaa("Accept("+value+")")
        noks = Nil
      }

    //Caso nosso valor seja aceite
    case AcceptOk(n) => {
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


