package sample.twitterstreams.queueprocessor

import java.util.Date
import java.util.concurrent.{BlockingQueue, TimeUnit}

import sample.twitterstreams.model.{DataPoint, Report}
import sample.twitterstreams.statisticsprocessor._

import scala.annotation.tailrec

/**
  * Created by lee on 11/1/16.
  */
class DataPointProcessor(dataPointQueue: BlockingQueue[DataPoint],
                         onReport: (Report) => Unit,
                         reportFrequencySeconds: Int,
                         isStoppingFn: () => Boolean) extends Runnable {

  val startTime = new Date

  val statProcessors: List[StatisticProcessor] = List(
   new TotalNumberOfTweetsProcessor,
   new AverageTweetsPerHourProcessor,
   new AverageTweetsPerMinProcessor,
   new AverageTweetsPerSecProcessor,
   new TopURLsInTweetsProcessor,
   new TopHashTagsInTweetsProcessor,
   new TopEmojisInTweetsProcessor
  )

  @tailrec
  final def applyDataPoint(report: Report,
                           dataPoint: DataPoint,
                           statisticProcessors: List[StatisticProcessor]): Report = {

    statisticProcessors match {
      case x :: xs =>
        applyDataPoint(
          x.applyDataPoint(report, dataPoint),
          dataPoint,
          xs)
      case nil => report
    }

  }

  override def run(): Unit = {
    var report: Report = initReport(new Report(Map()), statProcessors)
    while (!isStoppingFn()) {
      val dataPoint = dataPointQueue.take()
      report = applyDataPoint(report, dataPoint, statProcessors)
      onReport ( report )
    }
  }

  @tailrec
  private def processDataPoints(dataPointQueue: BlockingQueue[DataPoint],
                                report: Report,
                                statProcessors: List[StatisticProcessor],
                                isDoneFn: () => Boolean): Report = {

    (isDoneFn()) match {
      case true => return report;
      case false =>
        def dataPoint = dataPointQueue.poll(5, TimeUnit.SECONDS)
        dataPoint match {
          case null =>
            processDataPoints(dataPointQueue,
              report,
              statProcessors,
              isDoneFn)
          case dp: DataPoint =>
            processDataPoints(dataPointQueue,
              applyDataPoint(report, dataPoint, statProcessors),
              statProcessors,
              isDoneFn)
        }
    }
  }

  @tailrec
  private def initReport(report: Report,
                         statisticProcessors: List[StatisticProcessor]): Report = {
    statisticProcessors match {
      case x :: xs => initReport(x.init(report), xs)
      case nil => report
    }
  }
}
