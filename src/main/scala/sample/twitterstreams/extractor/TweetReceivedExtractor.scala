package sample.twitterstreams.extractor

import java.util.Date

import org.json4s._
import sample.twitterstreams.model.{DataPoint}

/**
  * Find hash tags in message.
  */
class TweetReceivedExtractor extends DataPointExtractor {
  override def extractDataPoints(json: JValue): List[DataPoint] = {
    val msgId = (json \ "id").values.toString()
    List(TweetReceived(msgId, new Date()))
  }
}
case class TweetReceived(tweetId: String, when: Date) extends DataPoint
