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

  var oks: Seq[(Option[Int], Option[Int])] = Nil
  var acs: Seq[Int] = Nil

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
    case PrepareOk(na, va) =>
      oks = ((na, va)) +: oks

      //Quorum
      if (oks.size > acceptors.size / 2) {
        //println("["+self.path.name+"] "+ oks.filter(_ != (None,None)))
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        val value = oks.filter(_ != (None,None))
          .sortBy { case (pN, _) => pN }
          .headOption
          .getOrElse((Some("tmp"),Some(v)))
          ._2
          .get
        botaa("Accept("+nn+", "+value+")")
        acceptors.foreach(_ ! Accept(nn, value))
        oks = Nil
      }

    //Caso de termos de enviar um novo prepare com um novo n
    case PrepareAgain =>
      quorum = quorum+1
      if (quorum > acceptors.size / 2) {
        nn = nn+1
        quorum = 0
        botap("Prepare("+nn+")")
        acceptors.foreach(_ ! Prepare(nn))
      }

    case AcceptOk(n) =>
      acs = n +: acs

      //Quorum
      if (acs.size > acceptors.size / 2) {
        botad("Decided("+v+")")
        acceptors.foreach(_ ! Decided(v))
        acs = Nil
        //Matar actor(?)
      }

  }
}


