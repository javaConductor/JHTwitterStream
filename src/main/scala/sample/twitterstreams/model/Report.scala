package sample.twitterstreams.model

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Accumulation of statistics
  */

 case class Report ( statistics : Map[String, Statistic], whenStarted: Date = new Date() ) {
  def createReport = {
    if( !statistics.isEmpty) {
      val sdf = new SimpleDateFormat("MMM dd, yyyy hh:mm:ssa")
      val nowString = sdf.format(new Date())
      val startString = sdf.format(whenStarted)
      def l: List[String] = statistics.values.toList map ({
        _.report()
      })
      ("-" * 20) + startString + " to " +  nowString + ("-" * 20) + "\n" + l.mkString("\n")
    }else{
      ""
    }
    }

}
