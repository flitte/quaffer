package com.flitte.quaffer.rdd

import com.flitte.quaffer.logging.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Simple RDD for Gaffer elements.
  */
class GafferRDD[T >: Element] (@transient sc: SparkContext)(implicit ct: ClassTag[T])
  extends RDD[T](sc, Seq.empty) with Logging {

  // make sure we inherit logging from the right place: out own Logging class and not RDD
  override def log = super[Logging].log

  override def logName = super[Logging].logName

  override def logInfo (msg: => String) = super[Logging].logInfo(msg)

  override def logDebug (msg: => String) = super[Logging].logDebug(msg)

  override def logTrace (msg: => String) = super[Logging].logTrace(msg)

  override def logWarning (msg: => String) = super[Logging].logWarning(msg)

  override def logError (msg: => String) = super[Logging].logError(msg)

  override def logInfo (msg: => String, throwable: Throwable) = super[Logging].logInfo(msg, throwable)

  override def logDebug (msg: => String, throwable: Throwable) = super[Logging].logDebug(msg, throwable)

  override def logTrace (msg: => String, throwable: Throwable) = super[Logging].logTrace(msg, throwable)

  override def logWarning (msg: => String, throwable: Throwable) = super[Logging].logWarning(msg, throwable)

  override def logError (msg: => String, throwable: Throwable) = super[Logging].logError(msg, throwable)

  override def isTraceEnabled () = super[Logging].isTraceEnabled()

  @DeveloperApi
  override def compute (split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions
}
