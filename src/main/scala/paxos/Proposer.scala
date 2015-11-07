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

  def receive = {

    // Enviar Prepare(n)
    case Start => acceptors.foreach(_ ! Prepare(n))

    // Esperar pelo PrepareOk(na, va)
    case PrepareOk(na, va) => {
      oks = ((na, va)) +: oks

      //Quorum
      if (oks.size > acceptors.size / 2) {
        //log.info("ja temos "+oks.size)
        //log.info(nn + " => " + oks.filter(_ != (None,None)).toString)
        //escolher o V da lista de oks com o N maior ou escolher o nosso v
        /*
        val value:Int = oks.filter(_ !=  (None, None))
          .sortBy { case (pN, _) => pN }
          .reverse
          .headOption
          .map { case (_, value) => value }
          .getOrElse(v)
        log.info(value.toString)
        */
        acceptors.foreach(_ ! Accept(nn, v))
        oks = Nil
      }
    }

    //Caso de termos de enviar um novo prepare com um novo n
    /*
    case PrepareAgain => {
      nn = nn+1
      acceptors.foreach(_ ! Prepare(nn))
    }
    */


    case AcceptOk(n) => {
      acs = n +: acs

      //Quorum
      if (acs.size > acceptors.size / 2) {
        acceptors.foreach(_ ! Decided(v))
      }
    }

  }

}


