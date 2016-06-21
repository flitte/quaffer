package com.flitte.quaffer.context

import com.flitte.gaffer.spark.config.ElementInputFormat
import com.flitte.quaffer.config.{BatchScannerElementInputFormat, ElementInputFormat, GraphConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
  * Class containing definitions for the additional functions which are available for the SparkContext object.
  */
class GafferContextFunctions(@transient val sc: SparkContext, @transient val store: AccumuloStore) extends Serializable {

  val config = new GraphConfig(store)

  protected def query(conf: Configuration, useBatchInputFormat: Boolean) = {
    if (useBatchInputFormat) {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[BatchScannerElementInputFormat], classOf[Element], classOf[Properties])
    } else {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[ElementInputFormat], classOf[Element], classOf[Properties])
    }
  }

  /**
    * Create an RDD of related edges, based on an initial seed value.
    *
    * @param seed the initial seed value
    * @param ct   ClassTag object containing the type of object to store in the RDD
    * @tparam T the type of object to store in the RDD
    * @return an RDD containing all edges related to the seed value
    */
  def relatedEdges[T >: Element](seed: String)(implicit ct: ClassTag[T]) = {
    val conf = new Configuration()
    config.setConfiguration(conf) // TODO
    query(conf, false)
  }

}
