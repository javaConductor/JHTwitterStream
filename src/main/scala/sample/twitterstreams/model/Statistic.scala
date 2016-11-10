package sample.twitterstreams.model

import java.io.OutputStream
import java.util.Date

import sample.twitterstreams.statisticsprocessor.StatisticProcessor

import scala.concurrent.duration.Duration

/**
  *
  */
trait Statistic {
  def report():String
}

