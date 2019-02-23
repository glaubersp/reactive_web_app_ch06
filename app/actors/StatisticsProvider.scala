package actors

import actors.StatisticsProvider._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor._
import javax.inject.Inject
import message.ComputeReach
import org.joda.time.{DateTime, Interval}
import play.api.libs.ws.WSClient
import reactivemongo.core.errors.ConnectionException

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class StatisticsProvider @Inject()(ws: WSClient, credentials: TwitterCredentials)
    extends Actor
    with ActorLogging {

  var reachComputer: ActorRef = _
  var storage: ActorRef = _
  var followersCounter: ActorRef = _

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  override def preStart(): Unit = {
    log.info("Starting StatisticsProvider")
    followersCounter =
      context.actorOf(UserFollowersCounter.props(ws, credentials), name = "userFollowersCounter")
    storage = context.actorOf(Props[Storage], name = "storage")
    reachComputer = context.actorOf(
      TweetReachComputer.props(followersCounter, storage, ws, credentials),
      name = "tweetReachComputer"
    )

    context.watch(storage)
  }

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 2.minutes) {
      case _: ConnectionException =>
        Restart
      case t: Throwable =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  def receive: PartialFunction[Any, Unit] = {
    case reach: ComputeReach =>
      log.info("Forwarding ComputeReach message to the reach computer")
      reachComputer forward reach
    case Terminated(_) =>
      context.system.scheduler.scheduleOnce(1.minute, self, ReviveStorage)
      context.become(storageUnavailable)
    case TwitterRateLimitReached(reset) =>
      context.system.scheduler.scheduleOnce(
        new Interval(DateTime.now, reset).toDurationMillis.millis,
        self,
        ResumeService
      )
      context.become(serviceUnavailable)
    case UserFollowersCounterUnavailable =>
      context.become(followersCountUnavailable)
  }

  def storageUnavailable: Receive = {
    case ComputeReach(_) =>
      sender() ! ServiceUnavailable
    case ReviveStorage =>
      storage = context.actorOf(Props[Storage], name = "storage")
      context.unbecome()
  }

  def serviceUnavailable: Receive = {
    case _: ComputeReach =>
      sender() ! ServiceUnavailable
    case ResumeService =>
      context.unbecome()
  }

  def followersCountUnavailable: Receive = {
    case UserFollowersCounterAvailable =>
      context.unbecome()
    case _: ComputeReach =>
      sender() ! ServiceUnavailable
  }

}

object StatisticsProvider {

  def props(ws: WSClient, credentials: TwitterCredentials): Props =
    Props(new StatisticsProvider(ws, credentials))

  case object ServiceUnavailable

  case object ReviveStorage

  case object ResumeService

}
