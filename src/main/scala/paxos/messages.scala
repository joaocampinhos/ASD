package paxos

case object Start

case class Prepare(n:Int)

case object PrepareAgain

case class PrepareOk(na:Option[Int], va:Option[Int])

case class Accept(n:Int, v:Int)

case class AcceptOk(n:Int)

case class Decided(n:Int)

