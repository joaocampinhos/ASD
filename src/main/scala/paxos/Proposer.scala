package paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import scala.util.Random

class Proposer(acceptors: Seq[ActorRef], n:Int) extends Actor {

  val log = Logging(context.system, this)

  //Nossa tag
  var nn:Int = n

  //Valor a propor
  val v:Int = Random.nextInt(10)

  var oks: Seq[Option[Proposal]] = Nil
  var acs: Seq[Int] = Nil

  var acsa: Seq[Int] = Nil

  var quorum = 0

  def botap(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.YELLOW+text+Console.WHITE) }
  def botaa(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.BLUE+text+Console.WHITE) }
  def botad(text: String) = { println(Console.CYAN+"["+self.path.name+"] "+Console.GREEN+text+Console.WHITE) }

  def receive = {

    // Enviar Prepare(n)
    case Start =>
      botap("Prepare("+n+")")
      acceptors.foreach(_ ! Prepare(n))

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
    case AcceptAgain(prop) => {}

    //Caso nosso valor seja aceite
    case AcceptOk(n) => {}
  }
}


