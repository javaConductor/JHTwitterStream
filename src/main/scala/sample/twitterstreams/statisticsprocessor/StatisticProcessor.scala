package sample.twitterstreams.statisticsprocessor

import sample.twitterstreams.model._

/**
  * Created by lee on 11/2/16.
  */
trait StatisticProcessor {
  val name:String
  def init(report: Report): Report
  def applyDataPoint(report:Report, dataPoint: DataPoint):Report
}
