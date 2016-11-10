package sample.twitterstreams

import java.io.FileReader
import java.util.{Date, Properties}

import sample.twitterstreams.model.Report
import sample.twitterstreams.service.TwitterStreamService

case class Config(consumerKey: String,
                  consumerSecret: String,
                  accessToken: String,
                  accessTokenSecret: String) {
}

/**
  * Created by lee on 10/30/16.
  */
object Main extends App {

  def createConfig(): Config = {
    val propertyFile = "app.properties"
    val props: Properties = new Properties();
    props.load(new FileReader(propertyFile))

    new Config(consumerKey = props.getProperty("consumerKey"),
      consumerSecret = props.getProperty("consumerSecret"),
      accessToken = props.getProperty("accessToken"),
      accessTokenSecret = props.getProperty("accessTokenSecret"))
  }

  val config: Config = createConfig()

  def printReport(report: Report): Unit = {
    println(report.createReport)
  }

  println("Creating TwitterStreamService")
  val service = new TwitterStreamService(15, config)
  service onReport printReport
  println("Starting TwitterStreamService")
  service.start()
  println("TwitterStreamService started")
}
