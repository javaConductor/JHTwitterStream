package sample.twitterstreams.service

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import sample.twitterstreams.Config
import sample.twitterstreams.model.{DataPoint, Report}

import scala.collection.mutable.ListBuffer

/**
  * Main service
  */
class TwitterStreamService(reportFrequencySeconds:Int, config: Config) {

  var listeners:ListBuffer[ Report => Unit ] = ListBuffer()
  val msgQueue = createMessageQueue(1000)
  val client = createClient(msgQueue)
  val isDoneFn = () => { client.isDone }
  val dataPointQueue = createDataPointQueue(1000)
  val messageService:MessageService = new MessageService(msgQueue, dataPointQueue, 2, isDoneFn )
  val reportQueue = createQueue[Report](1000)
  val dataPointService = new DataPointService(dataPointQueue, _onReport, reportFrequencySeconds, isDoneFn )

  def createQueue[T](nSize:Int):BlockingQueue[T] = new LinkedBlockingQueue[T](nSize)
  def createMessageQueue(nSize:Int):BlockingQueue[String] = createQueue[String](nSize)
  def createDataPointQueue(nSize:Int) = createQueue[DataPoint](nSize)
  def createClient(msgQueue:BlockingQueue[String]):Client = {
    val hosts:Hosts  = new HttpHosts("https://stream.twitter.com")
    val statusesSampleEndpoint:StatusesSampleEndpoint  = new StatusesSampleEndpoint()

    // These secrets should be read from a config file
    val authentication:Authentication  = new OAuth1(config.consumerKey,
      config.consumerSecret,
      config.accessToken,
      config.accessTokenSecret)
    val builder:ClientBuilder  = new ClientBuilder()
      .name("TwitterStreamService")
      .hosts(hosts)
      .authentication(authentication)
      .endpoint(statusesSampleEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))

    val client:Client  = builder.build()
    client
  }

  def start():Unit = {
    try {
      dataPointService.start()
      client.connect()
      println ("Endpoint: "+client.getEndpoint().getURI +" connected.")
    } catch {
      case e:Exception =>
        println(s"Could not connect endpoint: ${ client.getEndpoint().getURI } error: ${e.getClass}: ${e.getMessage}" )
        e.printStackTrace(System.err)
        System.exit(-1);
    }

    messageService.start()
  }

  /////////////////////////   Publishing
  private def _onReport(report:Report):Unit = {
    publishReport(report)
  }
  def onReport( listenerFn: Report => Unit ) = {
    listeners += listenerFn
  }
  def publishReport( report: Report ) = {
    listeners foreach  { _(report) }
  }
}
