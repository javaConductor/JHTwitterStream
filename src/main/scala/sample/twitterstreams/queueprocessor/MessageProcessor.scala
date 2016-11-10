package sample.twitterstreams.queueprocessor

import java.util.concurrent.BlockingQueue

import org.json4s.jackson._
import org.json4s.{JString, JValue, StringInput}
import sample.twitterstreams.extractor._
import sample.twitterstreams.model._

/**
  * Reads messages from the messageQueue and writes dataPoints to the dataPointQueue
  */
class MessageProcessor(
                        msgQueue: BlockingQueue[String],
                        dataPointQueue: BlockingQueue[DataPoint],
                        isDone: () => Boolean
                      ) extends Runnable {

  val extractors:List[DataPointExtractor] = List(
    new EmojisFoundExtractor,
    new HashTagsFoundExtractor,
    new TweetReceivedExtractor,
    new UrlFoundExtractor)

  override def run(): Unit = {
    println(s"MessageProcessor: ${Thread.currentThread().getId} starting.")
    while (!isDone()) {
      val msg = msgQueue.take()
      val dataPoints: List[DataPoint] = parseMessage(msg);
      val nWritten: Int = writeDataPoints(dataPoints, dataPointQueue)
    }
  }

  def writeDataPoints(dataPoints: List[DataPoint], dataPointQueue: BlockingQueue[DataPoint]): Int = {
    dataPoints.foreach(dataPointQueue.add)
    dataPoints.length
  }

  def parseDataPoints(json: JValue): List[DataPoint] = parseDataPoints(json, extractors)

  def parseDataPoints(json:JValue, extractors:List[DataPointExtractor], dataPoints:List[DataPoint] = List()):List[DataPoint] = {
    extractors match {
      case x :: xs => parseDataPoints(json, xs, x.extractDataPoints(json) ::: dataPoints)
      case nil => dataPoints
    }
  }

  def parseMessage(msg: String): List[DataPoint] = {
    val json: JValue = JsonMethods.parse(new StringInput(msg))
    (json \ "text").toOption match {
      case Some(v: JString) =>
        parseDataPoints(json)
      case _ =>
        List()
    }
  }
}
