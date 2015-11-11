package paxos

case object Start

case class Proposal(val n:Int, val v:Int)

case class Prepare(n:Int)

case class PrepareAgain(n:Option[Int])

case class PrepareOk(m:Option[Proposal])

case class Accept(m:Proposal)

case class AcceptOk(n:Int)

case class AcceptAgain(m:Option[Proposal])

case class Learn(n:Int)

