package paxos
import akka.actor._
import scala.collection.mutable.MutableList

case object Debug

case class Servers(s:MutableList[ActorSelection])

case class Operation(v:Any)

case object Start

case class Start(v:Any)

case object Stop

case object Output

case class Proposal(val n:Int, val v:Any)

case class Prepare(n:Int)

case class PrepareAgain(n:Option[Int])

case class PrepareOk(m:Option[Proposal])

case class Accept(m:Proposal)

case class AcceptOk(n:Int)

case class AcceptAgain(t: Int, m:Option[Proposal])

case class Learn(v:Any)
