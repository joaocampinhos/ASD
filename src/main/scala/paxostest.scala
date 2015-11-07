import akka.actor.{ActorSystem, Props, Actor, ActorRef, Deploy, AddressFromURIString}
import akka.remote.RemoteScope
import akka.event.Logging
import paxos._

object Paxos {

  def main(args: Array[String]) {

    val system = ActorSystem("paxos")

    //Criar 1 learner
    val learners = Seq(
      system.actorOf(Props(new Learner), name = "learner1")
    )

    //Criar 1 acceptor
    val acceptors = Seq(
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor1")
    )

    //Criar 3 proposers
    val proposers = Seq(
      system.actorOf(Props(new Proposer(acceptors)), name = "proposer1"),
      system.actorOf(Props(new Proposer(acceptors)), name = "proposer2"),
      system.actorOf(Props(new Proposer(acceptors)), name = "proposer3")
    )

    proposers.foreach(_ ! "start")

  }

}

