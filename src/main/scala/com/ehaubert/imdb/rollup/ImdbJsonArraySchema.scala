package com.ehaubert.imdb.rollup

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

object ImdbJsonArraySchema {
  //Used to explode json columns from the input data that are passed as an array of {id,name} objects
  val SPARK_SCHEMA = new ArrayType(
    elementType = new StructType()
      .add("id", LongType)
      .add("name", StringType),
    containsNull = false
  )
}
