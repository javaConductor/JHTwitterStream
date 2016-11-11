package sample.twitterstreams.service

import java.util.concurrent.{BlockingQueue, ExecutorService, Executors}

import sample.twitterstreams.model.DataPoint
import sample.twitterstreams.queueprocessor.MessageProcessor

/**
  * Message Processing:
  *   Reads from the Twitter Stream (msgQueue)
  *   Parses dataPoints from message
  *   Writes dataPoints to dataPointQueue
  *
  */
class MessageService(msgQueue: BlockingQueue[String],
                     dataPointQueue: BlockingQueue[DataPoint],
                     nThreads:Int,
                     isDone: () => Boolean) {

  val threadPool: ExecutorService = Executors.newFixedThreadPool(nThreads)
  def start(): Unit = {
    /// start nThreads threads
    (0 to nThreads) foreach ((n:Int) => {
      threadPool.execute(new MessageProcessor(msgQueue, dataPointQueue, isDone ));
    })
  }
}
