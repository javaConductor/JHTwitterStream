package sample.twitterstreams.tests

import java.util
import java.util.concurrent.ArrayBlockingQueue

import org.json4s.jackson.JsonMethods
import org.json4s.{StringInput, _}
import org.scalatest._
import org.scalatest.Matchers
import sample.twitterstreams.extractor.{HashTagsFound, HashTagsFoundExtractor}
import sample.twitterstreams.model.DataPoint

/**
  * Test parsing hashtags from tweet
  */
class ParseHashtagsSpec extends FlatSpec with Matchers {

    "HashTagsFoundExtractor" should "find 1 hashtags and return one HashTagDataPoint" in {
      val msgQ = new ArrayBlockingQueue[String](1)
      val dataPtQ = new util.concurrent.LinkedBlockingQueue[DataPoint]()
      val json: JValue = JsonMethods.parse(new StringInput( """{
      "id": "411031503817039874",
      "id_str": "411031503817039874",
		  "text": "This is one URL http://linkedin.com",
      "entities": {
      |        "hashtags": [{
      |            "text": "hashtag",
      |            "indices": [23, 31]
      |        }]},
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
		}
                                                            """.stripMargin))
      val extractor = new HashTagsFoundExtractor()
      val dataPoints = extractor.extractDataPoints(json)
      dataPoints shouldBe a [List[_]]
      dataPoints.head shouldBe a [HashTagsFound]
      dataPoints should have length 1
    }
  }
