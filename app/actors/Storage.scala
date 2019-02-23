package actors

import java.time._

import actors.Storage._
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.pipe
import message.{ReachStored, StoreReach}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson._
import reactivemongo.core.errors.ConnectionException

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

case class StoredReach(when: LocalDateTime, tweetId: BigInt, score: Int)

object StoredReach {

  implicit object BigIntHandler extends BSONDocumentReader[BigInt] with BSONDocumentWriter[BigInt] {

    def write(bigInt: BigInt): BSONDocument =
      BSONDocument(
        "signum" -> bigInt.signum,
        "value"  -> BSONBinary(bigInt.toByteArray, Subtype.UserDefinedSubtype)
      )

    def read(doc: BSONDocument): BigInt =
      BigInt(doc.getAs[Int]("signum").get, {
        val buf = doc.getAs[BSONBinary]("value").get.value
        buf.readArray(buf.readable())
      })

  }

  implicit object StoredReachHandler
      extends BSONDocumentReader[StoredReach]
      with BSONDocumentWriter[StoredReach] {

    override def read(bson: BSONDocument): StoredReach = {
      val when = bson
        .getAs[BSONDateTime]("when")
        .map(
          t =>
            LocalDateTime
              .ofInstant(Instant.ofEpochMilli(t.value), ZoneOffset.UTC)
        )
        .get
      val tweetId = bson.getAs[BigInt]("tweet_id").get
      val score = bson.getAs[Int]("score").get
      StoredReach(when, tweetId, score)
    }

    override def write(r: StoredReach): BSONDocument =
      BSONDocument(
        "when"     -> BSONDateTime(r.when.toInstant(ZoneOffset.UTC).toEpochMilli),
        "tweetId"  -> r.tweetId,
        "tweet_id" -> r.tweetId,
        "score"    -> r.score
      )
  }

}

class Storage() extends Actor with ActorLogging {

  val Database = "twitterService"
  val ReachCollection = "ComputedReach"

  implicit val executionContext: ExecutionContextExecutor = context.dispatcher
  implicit val timeout: FiniteDuration = 10.second

  val driver: MongoDriver = new MongoDriver
  var connection: MongoConnection = _
  var db: DefaultDB = _
  var collection: BSONCollection = _

  obtainConnection()

  var currentWrites = Set.empty[BigInt]

  override def postRestart(reason: Throwable): Unit = {
    reason match {
      case _: ConnectionException =>
        obtainConnection()
    }
    super.postRestart(reason)
  }

  private def obtainConnection(): Unit = {
    connection = driver.connection(List("localhost"))
    db = Await.result(connection.database(Database), 10.seconds)
    collection = db.collection[BSONCollection](ReachCollection)
  }

  override def postStop(): Unit = {
    connection.askClose()
    driver.close()
  }

  def receive: PartialFunction[Any, Unit] = {
    case StoreReach(tweetId, score) =>
      log.info("Storing reach for tweet {} - SCORE: {}", tweetId, score)
      if (!currentWrites.contains(tweetId)) {
        currentWrites = currentWrites + tweetId
        val originalSender = sender()
        collection.insert
          .one(StoredReach(LocalDateTime.now, tweetId, score))
          .map { lastError =>
            LastStorageError(lastError, tweetId, originalSender)
          }
          .recover {
            case _ =>
              currentWrites = currentWrites - tweetId
          } pipeTo self
      }
    case LastStorageError(error, tweetId, client) =>
      if (!error.ok) {
        currentWrites = currentWrites - tweetId
      } else {
        client ! ReachStored(tweetId)
      }
  }
}

object Storage {

  case class LastStorageError(result: WriteResult, tweetId: BigInt, client: ActorRef)

}
