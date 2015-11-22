package paxos
import akka.actor._
import scala.collection.mutable.MutableList

case object Debug

case class Servers(s:Seq[ActorRef])
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

case class AcceptAgain(m: Option[Proposal])

case class Learn(v: Any)

abstract class Action

case class Get(key: String) extends Action

case class Put(key: String, value: String) extends Action

case object WhoIsLeader

case class TheLeaderIs(l: ActorRef)

case object DoRequest

case object Alive
