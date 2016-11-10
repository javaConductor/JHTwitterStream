package sample.twitterstreams.extractor

import org.json4s.JValue
import sample.twitterstreams.model.DataPoint

/**
  * Created by lcollins on 11/10/2016.
  */
trait DataPointExtractor {
  def extractDataPoints(json:JValue): List[DataPoint]
}
