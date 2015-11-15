import akka.actor._
import akka.remote.RemoteScope
import akka.event.Logging
import paxos._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable.MutableList

object Paxos {

  def main(args: Array[String]) {

    val system = ActorSystem("paxos")

    val t = system.actorOf(Props(new T), name = "Test1")

    t ! Start

  }

  class T extends Actor {

    val system = ActorSystem("paxos")

    var count:Int = 0

    //Criar 1 learner
    val learners = MutableList(
      system.actorSelection(system.actorOf(Props(new Learner), name = "learner1").path),
      system.actorSelection(system.actorOf(Props(new Learner), name = "learner2").path),
      system.actorSelection(system.actorOf(Props(new Learner), name = "learner3").path)
    )

    //Criar 1 acceptor
    val acceptors = MutableList(
      system.actorSelection(system.actorOf(Props(new Acceptor), name = "acceptor1").path),
      system.actorSelection(system.actorOf(Props(new Acceptor), name = "acceptor2").path),
      system.actorSelection(system.actorOf(Props(new Acceptor), name = "acceptor3").path)
    )

    //Criar 3 proposers
    val proposers = MutableList(
      system.actorSelection(system.actorOf(Props(new Proposer), name = "proposer1").path),
      system.actorSelection(system.actorOf(Props(new Proposer), name = "proposer2").path),
      system.actorSelection(system.actorOf(Props(new Proposer), name = "proposer3").path)
    )

    proposers.foreach(_ ! Servers(acceptors))
    acceptors.foreach(_ ! Servers(learners))

    proposers.head ! Operation("Olá")
    proposers.tail.head ! Operation("Olé")
    proposers.tail.tail.head ! Operation("Olí")

    //proposers.foreach(_ ! Debug)
    //acceptors.foreach(_ ! Debug)

    var done = false

    def receive = {
      case Start =>
        proposers.foreach(_ ! Start)

      case Learn(v) =>
        count = count + 1

        if (count == learners.size) {
          count = 0
          println(v)
          for (p <- proposers) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
          for (p <- acceptors) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
          for (p <- learners) {
            implicit val timeout = Timeout(20 seconds)
            val future = p ? Stop
            val result = Await.result(future, timeout.duration)
          }
          if (!done) {
            proposers.head ! Operation(1)
            proposers.tail.head ! Operation(2)
            proposers.tail.tail.head ! Operation(3)
            done = true
            self ! Start
          }
        }
    }
  }

}

