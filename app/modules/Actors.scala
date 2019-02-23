package modules

import actors.{StatisticsProvider, TwitterCredentials}
import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import javax.inject._
import play.api.libs.ws.WSClient

class Actors @Inject()(system: ActorSystem, ws: WSClient, credentials: TwitterCredentials)
    extends ApplicationActors {

  system.actorOf(
    props = StatisticsProvider.props(ws, credentials).withDispatcher("control-aware-dispatcher"),
    name = "statisticsProvider"
  )

}

trait ApplicationActors

class ActorsModule extends AbstractModule {

  override def configure(): Unit = {
    bind(classOf[ApplicationActors]).to(classOf[Actors]).asEagerSingleton()
  }

}
