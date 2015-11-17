package paxos

import akka.actor.ActorRef
import scala.collection.mutable.MutableList


case class Write(ket: Int, value: String) extends Action {}

case class Read(key: Int) extends Action {}

case class View(id: Long, participants: MutableList[ActorRef], state: MutableList[Action] ) {}
