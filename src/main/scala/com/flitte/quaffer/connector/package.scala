package com.flitte.quaffer

import com.flitte.quaffer.context.GafferContextFunctions
import org.apache.spark.SparkContext

/**
  * The root package of the Gaffer connector for Apache Spark.
  *
  * Provides implicit conversions which add Gaffer-specific functions to [[org.apache.spark.SparkContext SparkContext]].
  */
package object connector {

  implicit def toContextFunctions (sc: SparkContext): GafferContextFunctions = {
    new GafferContextFunctions(sc)
  }

}
