package actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.dispatch.ControlMessage
import akka.pattern.{CircuitBreaker, pipe}
import javax.inject.Inject
import message.{FetchFollowerCount, FollowerCount}
import org.joda.time.DateTime
import play.api.libs.oauth.OAuthCalculator
import play.api.libs.ws.WSClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class UserFollowersCounter @Inject()(ws: WSClient, credentials: TwitterCredentials) extends Actor with ActorLogging {

  implicit val executionContext: ExecutionContextExecutor = context.dispatcher
  val breaker =
    new CircuitBreaker(context.system.scheduler,
      maxFailures = 5,
      callTimeout = 2.seconds,
      resetTimeout = 1.minute
    )
  val LimitRemaining = "X-Rate-Limit-Remaining"
  val LimitReset = "X-Rate-Limit-Reset"

  def receive: PartialFunction[Any, Unit] = {
    case FetchFollowerCount(tweetId, user) =>
      val originalSender = sender()
      breaker.onOpen({
        log.info("Circuit breaker open")
        originalSender ! FollowerCountUnavailable(tweetId, user)
        context.parent ! UserFollowersCounterUnavailable
      }).onHalfOpen(
        log.info("Circuit breaker half-open")
      ).onClose({
        log.info("Circuit breaker closed")
        context.parent ! UserFollowersCounterAvailable
      }).withCircuitBreaker(fetchFollowerCount(tweetId, user)) pipeTo sender()
  }

  private def fetchFollowerCount(tweetId: BigInt, userId: BigInt): Future[FollowerCount] = {
    credentials.getCredentials.map {
      case (consumerKey, requestToken) =>
        ws.url("https://api.twitter.com/1.1/users/show.json")
          .sign(OAuthCalculator(consumerKey, requestToken))
          .addQueryStringParameters("user_id" -> userId.toString)
          .get().map { response =>
          if (response.status == 200) {

            val rateLimit = for {
              remaining <- response.header(LimitRemaining)
              reset <- response.header(LimitReset)
            } yield {
              (remaining.toInt, new DateTime(reset.toLong * 1000))
            }

            rateLimit.foreach { case (remaining, reset) =>
              log.info("Rate limit: {} requests remaining, window resets at {}", remaining, reset)
              if (remaining < 50) {
                Thread.sleep(10000)
              }
              if (remaining < 10) {
                context.parent ! TwitterRateLimitReached(reset)
              }
            }

            val count = (response.json \ "followers_count").as[Int]
            FollowerCount(tweetId, userId, count)
          } else {
            throw new RuntimeException(s"Could not retrieve followers count for user $userId")
          }
        }
    }.getOrElse {
      Future.failed(new RuntimeException("You did not correctly configure the Twitter credentials"))
    }
  }

}

object UserFollowersCounter {
  def props(ws: WSClient, credentials: TwitterCredentials): Props = Props(new UserFollowersCounter(ws, credentials))
}

case class TwitterRateLimitReached(reset: DateTime) extends ControlMessage

case class FollowerCountUnavailable(tweetId: BigInt, user: BigInt)

case object UserFollowersCounterUnavailable extends ControlMessage

case object UserFollowersCounterAvailable extends ControlMessage