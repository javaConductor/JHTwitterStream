package sample.twitterstreams.service

import java.util.concurrent.{BlockingQueue, Executors, TimeUnit}

import sample.twitterstreams.model.{DataPoint, Report}
import sample.twitterstreams.queueprocessor.DataPointProcessor

/**
  * Data Point Processing:
  *   Reads from dataPoint Queue
  *   Create new report from applying new dataPoints to current report
  *   Writes dataPoints to dataPointQueue
  */
class DataPointService(dataPointQueue: BlockingQueue[DataPoint],
                       onReport: (Report) => Unit,
                       reportFrequencySeconds:Int,
                       isDone: () => Boolean) {

  var currentReport = new Report(Map())
  def start(): Unit ={
  //  println ("Starting DataPointProcessor()")
      new Thread(new DataPointProcessor( dataPointQueue,
        _onReport,
        reportFrequencySeconds,
        isDone )).start();
    publishReports(reportFrequencySeconds)
  }

  /**
    * Called when DataPointProcessor creates a new Report
    *
    * @param report
    */
  private def _onReport(report:Report): Unit = {
    currentReport = report
  }

  def publishReports(reportFrequencySeconds: Int) = {
    val task = new Runnable {
      def run() = onReport (currentReport)
    }
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(task,
                                                            reportFrequencySeconds,
                                                            reportFrequencySeconds,
                                                            TimeUnit.SECONDS)
  }

}
