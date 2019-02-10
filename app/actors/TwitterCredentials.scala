package actors

import javax.inject.{Inject, Singleton}
import play.api.Configuration
import play.api.libs.oauth.{ConsumerKey, RequestToken}

@Singleton
class TwitterCredentials @Inject()(config: Configuration) {

  def getCredentials: Option[(ConsumerKey, RequestToken)] = {
    val apiKey = config.get[String]("twitter.apiKey")
    val apiSecret = config.get[String]("twitter.apiSecret")
    val token = config.get[String]("twitter.accessToken")
    val tokenSecret = config.get[String]("twitter.accessTokenSecret")
    Some(ConsumerKey(apiKey, apiSecret), RequestToken(token, tokenSecret))
  }
}

