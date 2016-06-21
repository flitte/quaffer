# quaffer
A native Spark connector for the Gaffer graph framework.

## Overview
The quaffer project aims to provide a native method for generating Spark RDDs from Gaffer Element objects.

## Usage
Example of using quaffer with the Spark shell:

```scala
scala> sc.stop
scala> import com.flitte.quaffer.connector._
scala> import org.apache.spark.SparkContext
scala> val sc = new SparkContext()
scala> val rdd = sc.relatedEdges("SEED")
com.flitte.quaffer.rdd.GafferRDD[Element] = com.flitte.quaffer.rdd.GafferRDD[Element]@2ee9b6e3
```
