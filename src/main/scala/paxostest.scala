import akka.actor._
import akka.remote.RemoteScope
import akka.event.Logging
import paxos._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.collection.mutable.MutableList
import collection.immutable.HashMap

object Paxos {

  case class ServersConf(servers: HashMap[String, ActorRef])

  def main(args: Array[String]) {

    val system = ActorSystem("paxos")

    val t = system.actorOf(Props(new T), name = "Test1")
    val t1 = system.actorOf(Props(new T), name = "Test2")

    var serversMap = HashMap[String, ActorRef](
      t.path.name -> t,
      t1.path.name -> t1
    )

    t ! ServersConf(serversMap)
    t1 ! ServersConf(serversMap)
    //t ! Start
    //t1 ! Start

  }

  class T extends Actor {
    import context.dispatcher

    var count:Int = 0

    //Criar 1 learner
    val learners = MutableList(
      context.actorSelection(context.actorOf(Props(new Learner), name = "learner").path)
    )

    //Criar 1 acceptor
    val acceptors = MutableList(
      context.actorSelection(context.actorOf(Props(new Acceptor), name = "acceptor").path)
    )

    //Criar 3 proposers
    val proposers = MutableList(
      context.actorSelection(context.actorOf(Props(new Proposer), name = "proposer").path)
    )

    val paxos = new MutableList[ActorSelection]()

    //proposers.foreach(_ ! Servers(acceptors))
    //acceptors.foreach(_ ! Servers(learners))
    //learners.foreach(_ ! Servers(MutableList(context.actorSelection(self.path))))

    proposers.head ! Operation("Olá")
    proposers.tail.head ! Operation("Olé")
    proposers.tail.tail.head ! Operation("Olí")

    //proposers.foreach(_ ! Debug)
    //acceptors.foreach(_ ! Debug)

    var done = false

    def receive = {

      case ServersConf(s) =>
        println(s)
        s.values.foreach(e => {
          proposers += context.actorSelection(e.path + "/Paxos/proposer")
          acceptors += context.actorSelection(e.path + "/Paxos/acceptor")
          learners += context.actorSelection(e.path + "/Paxos/learner")
          paxos += context.actorSelection(e.path + "/Paxos")
        })
        proposers.foreach(_ ! Servers(acceptors))
        acceptors.foreach(_ ! Servers(learners))
        learners.foreach(_ ! Servers(paxos))

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

