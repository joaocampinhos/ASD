package deploy

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object Base {
  def main(args: Array[String]) {
    val config = ConfigFactory.load("base")
    val system = ActorSystem(
      config.getString(args.head + ".name"),
      config.getConfig(args.head).withFallback(config)
    )
  }
}

