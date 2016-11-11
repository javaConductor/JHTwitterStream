package sample.twitterstreams.tests
import java.util
import java.util.concurrent.ArrayBlockingQueue

import org.json4s.jackson.JsonMethods
import org.json4s.{StringInput, _}
import org.scalatest._
import org.scalatest.Matchers
import sample.twitterstreams.extractor.{UrlFound, UrlFoundExtractor}
import sample.twitterstreams.model.DataPoint

/**
  * Created by lcollins on 11/7/2016.
  */
class ParseURLSpec extends FlatSpec with Matchers {

    "MessageProcessor" should "find 2 URLs and return one URL dataPoint" in {
      val msgQ = new ArrayBlockingQueue[String](1)
      val dataPtQ = new util.concurrent.LinkedBlockingQueue[DataPoint]()
      val json: JValue = JsonMethods.parse(new StringInput(
                                                            """{
      "id": "411031503817039874",
      "id_str": "411031503817039874",
		  "text": "This is one URL http://linkedin.com",
	    "entities" : {
	    "urls": [{
      |            "url": "http:\/\/t.co\/p5dOtmnZyu",
      |            "expanded_url": "http:\/\/dev.twitter.com",
      |            "display_url": "dev.twitter.com",
      |            "indices": [32, 54]
      |        }, {
      |            "url": "https:\/\/t.co\/ZSvIEMOPb8",
      |            "expanded_url": "https:\/\/ton.twitter.com\/1.1\/ton\/data\/dm\/411031503817039874\/411031503833792512\/cOkcq9FS.jpg",
      |            "display_url": "pic.twitter.com\/ZSvIEMOPb8",
      |            "indices": [55, 78]
      |        }]
	  |        }
		}""".stripMargin))
      val extractor = new UrlFoundExtractor

      val dataPoints = extractor.extractDataPoints(json)
      dataPoints shouldBe a [List[_]]
      dataPoints.head shouldBe a [UrlFound]
      dataPoints should have length 1
    }

  }
