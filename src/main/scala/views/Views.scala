package views
import akka.actor._
import scala.collection.mutable.MutableList

object Views {
  case class ConfigServer(serverId: Int, servers: collection.immutable.Seq[ActorRef], coordinator: ActorRef)

  case class OperationSuccessful(message: String)

  case class OperationError(message: String)

  case class ConfigureCoordinator(servers: List[ActorRef], clients: List[ActorRef])

  case object StartElection

  case class LeaderElected(leader: ActorRef)

  case object GetViewId

  case class ViewId(id: Int)

  case object GetOperationId

  case class OperationId(id: Int)

  case class Request(op: ViewsOperation)

  trait ViewsOperation {
    def id: Int
    def hash: Int
  }
  case class View(id: Int, leader: ActorRef, participants: List[ActorRef], state: List[ViewsOperation])
  case class Write(id: Int,hash:Int, key: String, value: String) extends ViewsOperation
  case class Read(id: Int,hash:Int, key: String) extends ViewsOperation
  case class ServerDetails(keyHash: Int, id: Int, view: View)
  case class JoinView(keyHash: Int, id: Int, who: ActorRef)
  case class UpdateView(keyHash: Int, view: View)
}
