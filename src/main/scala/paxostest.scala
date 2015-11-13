import akka.actor.{ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString}
import akka.remote.RemoteScope
import akka.event.Logging
import paxos._

object Paxos {

  def main(args: Array[String]) {

    val system = ActorSystem("paxos")

    //Criar 1 learner
    val learners = Seq(
      system.actorOf(Props(new Learner), name = "learner1"),
      system.actorOf(Props(new Learner), name = "learner2"),
      system.actorOf(Props(new Learner), name = "learner3")
    )

    //Criar 1 acceptor
    val acceptors = Seq(
      system.actorOf(Props(new Acceptor), name = "acceptor1"),
      system.actorOf(Props(new Acceptor), name = "acceptor2"),
      system.actorOf(Props(new Acceptor), name = "acceptor3")
    )

    //Criar 3 proposers
    val proposers = Seq(
      system.actorOf(Props(new Proposer), name = "proposer1"),
      system.actorOf(Props(new Proposer), name = "proposer2"),
      system.actorOf(Props(new Proposer), name = "proposer3")
    )

    proposers.foreach(_ ! Servers(acceptors))
    acceptors.foreach(_ ! Servers(learners))

    proposers.head ! Operation("Olá")
    proposers.tail.head ! Operation("Olé")
    proposers.tail.tail.head ! Operation("Olí")

    //proposers.foreach(_ ! Debug)
    //acceptors.foreach(_ ! Debug)

    proposers.foreach(_ ! Start)

  }

}

