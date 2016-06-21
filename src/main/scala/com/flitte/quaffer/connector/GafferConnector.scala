package com.flitte.quaffer.connector

import com.flitte.quaffer.config.{BatchScannerElementInputFormat, ElementInputFormat, GraphConfig}
import com.flitte.quaffer.logging.Logging

/**
  * @author flitte
  * @since 08/06/2016.
  */
class GafferConnector(conf: GraphConfig) extends Serializable with Logging {

  protected def query(conf: Configuration, useBatchInputFormat: Boolean) = {
    if (useBatchInputFormat) {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[BatchScannerElementInputFormat], classOf[Element], classOf[Properties])
    } else {
      sc.newAPIHadoopRDD(new Configuration(conf), classOf[ElementInputFormat], classOf[Element], classOf[Properties])
    }
  }

}
