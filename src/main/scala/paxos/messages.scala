package paxos
import akka.actor._
import scala.collection.mutable.MutableList

case object Debug

case class Servers(s: Seq[ActorRef])
// case class Servers(s: MutableList[ActorRef])

case class Operation(v: Any) extends Action

case object Start

case object Go

case class Start(v: Any)

case object Stop

case object Output

case class Proposal(val n: Int, val v: Any)

case class Prepare(n: Int)

case class PrepareAgain(n: Option[Int])

case class PrepareOk(m: Option[Proposal])

case class Accept(m: Proposal)

case class AcceptOk(n: Int)

case class AcceptAgain(t: Int, m: Option[Proposal])

case class Learn(v: Any)

abstract class Action

case class Get(key: String) extends Action

case class Put(key: String, value: String) extends Action

case object WhoIsLeader

case class TheLeaderIs(l: ActorRef)

case object DoRequest

case object Alive

case object Timeout

case object Shutdown

case class ConfigServer(serverId: Int, servers: collection.immutable.Seq[ActorRef], coordinator: ActorRef)

trait ViewsOperation {
def id: Int
}

case class Write(id: Int, key: String, value: String) extends ViewsOperation 
case class Read(id: Int, key: String) extends  ViewsOperation
case class OperationSuccessful(message: String)
case class OperationError(message: String)

case class JoinView(serverId: Int, who: ActorRef)
case class UpdateView(newView: View)


case class ConfigureCoordinator(servers: List[ActorRef], clients: List[ActorRef])

case class View(id: Int, leader: ActorRef, participants: List[ActorRef], state: List[ViewsOperation])

case class ServerDetails(serverId: Int, currentView: View)

case object StartElection

case class LeaderElected(leader: ActorRef)

case object GetViewId

case class ViewId(id: Int)

case object GetOperationId

case class OperationId(id: Int)
case class Request(op: ViewsOperation)

