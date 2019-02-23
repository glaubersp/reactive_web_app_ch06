package actors

import akka.actor._
import akka.pattern.pipe
import javax.inject.Inject
import message._
import play.api.libs.json.JsArray
import play.api.libs.oauth.OAuthCalculator
import play.api.libs.ws.WSClient

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

class TweetReachComputer @Inject()(
  userFollowersCounter: ActorRef,
  storage: ActorRef,
  ws: WSClient,
  credentials: TwitterCredentials
)(implicit ec: ExecutionContext)
    extends Actor
    with ActorLogging {

  val retryScheduler: Cancellable =
    context.system.scheduler.schedule(10.millis, 20.seconds, self, ResendUnacknowledged)

  implicit val executionContext: ExecutionContextExecutor = context.dispatcher

  var followerCountsByRetweet = Map.empty[FetchedRetweet, List[FollowerCount]]

  override def postStop(): Unit = {
    retryScheduler.cancel()
  }

  def receive: PartialFunction[Any, Unit] = {

    case ComputeReach(tweetId) =>
      log.info("Starting to compute tweet reach for tweet {}", tweetId)
      val originalSender = sender()
      fetchRetweets(tweetId, sender()).recover {
        case NonFatal(t) =>
          RetweetFetchingFailed(tweetId, t, originalSender)
      } pipeTo self

    case fetchedRetweets: FetchedRetweet =>
      log.info("Received retweets for tweet {}", fetchedRetweets.tweetId)
      followerCountsByRetweet =
      followerCountsByRetweet + (fetchedRetweets -> List.empty)
      fetchedRetweets.retweeters.foreach { rt =>
        userFollowersCounter ! FetchFollowerCount(fetchedRetweets.tweetId, rt)
      }

    case count @ FollowerCount(tweetId, userId, followersCount) =>
      log.info(
        "Received followers count for tweet {}: userId = {} - COUNT: {}",
        tweetId,
        userId,
        followersCount
      )
      fetchedRetweetsFor(tweetId).foreach { fetchedRetweets =>
        updateFollowersCount(tweetId, fetchedRetweets, count)
      }

    case FollowerCountUnavailable(tweetId, _) =>
      followerCountsByRetweet.keys.find(_.tweetId == tweetId).foreach { fetchedRetweets =>
        fetchedRetweets.client ! TweetReachCouldNotBeComputed
      }

    case ReachStored(tweetId) =>
      log.info("Received confirmation of storage for tweet {}", tweetId)
      followerCountsByRetweet.keys.find(_.tweetId == tweetId).foreach { key =>
        followerCountsByRetweet = followerCountsByRetweet.filterNot(_._1 == key)
      }

    case RetweetFetchingFailed(tweetId, cause, _) =>
      log.error(cause, "Could not fetch retweets for tweet {}", tweetId)

    case ResendUnacknowledged =>
      val unacknowledged = followerCountsByRetweet.filterNot {
        case (retweet, counts) =>
          retweet.retweeters.size != counts.size
      }
      unacknowledged.foreach {
        case (retweet, counts) =>
          storage ! StoreReach(retweet.tweetId, counts.map(_.followersCount).sum)
      }

  }

  def fetchedRetweetsFor(tweetId: BigInt): Option[FetchedRetweet] =
    followerCountsByRetweet.keys.find(_.tweetId == tweetId)

  def updateFollowersCount(
    tweetId: BigInt,
    fetchedRetweets: FetchedRetweet,
    count: FollowerCount
  ): Unit = {
    val existingCounts = followerCountsByRetweet(fetchedRetweets)
    followerCountsByRetweet =
      followerCountsByRetweet.updated(fetchedRetweets, count :: existingCounts)
    val newCounts = followerCountsByRetweet(fetchedRetweets)
    if (newCounts.length == fetchedRetweets.retweeters.length) {
      log.info("Received all retweeters followers count for tweet {}, computing sum", tweetId)
      val score = newCounts.map(_.followersCount).sum
      fetchedRetweets.client ! TweetReach(tweetId, score)
      storage ! StoreReach(tweetId, score)
    }
  }

  def fetchRetweets(tweetId: BigInt, client: ActorRef): Future[FetchedRetweet] = {
    credentials.getCredentials
      .map {
        case (consumerKey, requestToken) =>
          ws.url("https://api.twitter.com/1.1/statuses/retweeters/ids.json")
            .sign(OAuthCalculator(consumerKey, requestToken))
            .addQueryStringParameters("id" -> tweetId.toString)
            .addQueryStringParameters("stringify_ids" -> "true")
            .get()
            .map { response =>
              if (response.status == 200) {
                val ids = (response.json \ "ids")
                  .as[JsArray]
                  .value
                  .map(v => BigInt(v.as[String]))
                  .toList
                FetchedRetweet(tweetId, ids, client)
              } else {
                throw new RuntimeException(s"Could not retrieve details for tweet $tweetId")
              }
            }
      }
      .getOrElse {
        Future.failed(
          new RuntimeException("You did not correctly configure the Twitter credentials")
        )
      }
  }

  case class FetchedRetweet(tweetId: BigInt, retweeters: List[BigInt], client: ActorRef)

  case class RetweetFetchingFailed(tweetId: BigInt, cause: Throwable, client: ActorRef)

  case object ResendUnacknowledged

}

object TweetReachComputer {

  def props(
    followersCounter: ActorRef,
    storage: ActorRef,
    ws: WSClient,
    credentials: TwitterCredentials
  )(implicit ec: ExecutionContext): Props =
    Props(new TweetReachComputer(followersCounter, storage, ws, credentials))
}
