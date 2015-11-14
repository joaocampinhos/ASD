package paxos

import akka.actor.ActorRef

abstract class Operation {}

case class Write(ket: Int, value: String) extends Operation {}

case class Read(key: Int) extends Operation {}

case class View(id: Long, participants: List[ActorRef], state: List[Operation] ) {}