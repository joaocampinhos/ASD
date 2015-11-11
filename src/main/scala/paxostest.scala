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
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor1"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor2"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor4"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor5"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor6"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor7"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor8"),
      system.actorOf(Props(new Acceptor(learners)), name = "acceptor9")
    )

    //Criar 3 proposers
    val proposers = Seq(
      system.actorOf(Props(new Proposer(acceptors, 1)), name = "proposer1"),
      system.actorOf(Props(new Proposer(acceptors, 2)), name = "proposer2"),
      system.actorOf(Props(new Proposer(acceptors, 3)), name = "proposer4"),
      system.actorOf(Props(new Proposer(acceptors, 4)), name = "proposer5"),
      system.actorOf(Props(new Proposer(acceptors, 5)), name = "proposer6"),
      system.actorOf(Props(new Proposer(acceptors, 6)), name = "proposer7"),
      system.actorOf(Props(new Proposer(acceptors, 7)), name = "proposer8")
    )

    proposers.foreach(_ ! Start)

  }

}

