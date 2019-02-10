package controllers

import actors.StatisticsProvider
import akka.actor.{ActorSelection, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import javax.inject._
import message.{ComputeReach, TweetReach, TweetReachCouldNotBeComputed}
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class Application @Inject()(controllerComponents: ControllerComponents, system: ActorSystem)(implicit ec: ExecutionContext) extends AbstractController(controllerComponents) {

  lazy val statisticsProvider: ActorSelection = system.actorSelection("akka://application/user/statisticsProvider")

  def computeReach(tweetId: String): Action[AnyContent] = Action.async {
    implicit val timeout: Timeout = Timeout(5.minutes)
    val eventuallyReach = statisticsProvider ? ComputeReach(BigInt(tweetId))
    eventuallyReach.map {
      case tr: TweetReach =>
        Ok(tr.score.toString)
      case StatisticsProvider.ServiceUnavailable =>
        ServiceUnavailable("Sorry")
      case TweetReachCouldNotBeComputed =>
        ServiceUnavailable("Sorry")
    }
  }

}