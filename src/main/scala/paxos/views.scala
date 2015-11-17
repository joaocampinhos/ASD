package paxos

import akka.actor.ActorRef


case class Write(ket: Int, value: String) extends Action {}

case class Read(key: Int) extends Action {}

case class View(id: Long, participants: List[ActorRef], state: List[Action] ) {}
