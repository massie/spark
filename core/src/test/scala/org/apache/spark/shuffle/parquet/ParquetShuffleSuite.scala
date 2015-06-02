/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.parquet

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.parquet.avro.AvroTestEntity
import org.apache.spark._

class ParquetShuffleSuite extends SparkFunSuite with LocalSparkContext {

  val conf = new SparkConf()
  conf.set(ParquetShuffleConfig.SPARK_SHUFFLE_MANAGER_KEY, classOf[ParquetShuffleManager].getName)
  conf.set("spark.serializer", classOf[KryoSerializer].getName)

  test("metrics for shuffle without aggregation (ints, sort shuffle)") {
    sc = new SparkContext("local", "test", conf.clone())
    val numRecords = 10000

    val metrics = ShuffleSuite.runAndReturnMetrics(sc) {
      sc.parallelize(1 to numRecords, 4)
        .map(key => (key, 1))
        .groupByKey()
        .collect()
    }

    assert(metrics.recordsRead === numRecords)
    assert(metrics.recordsWritten === numRecords)
    assert(metrics.bytesWritten === metrics.byresRead)
    assert(metrics.bytesWritten > 0)
  }

  test("metrics for shuffle without aggregation (avro object, parquet shuffle)") {
    sc = new SparkContext("local", "test", conf.clone())
    val numRecords = 10000
    val records = for (i <- 1 to numRecords) yield {
      val obj = AvroTestEntity.newBuilder().setA("test").setB(i.toLong).build()
      (obj, obj)
    }
    val metrics = ShuffleSuite.runAndReturnMetrics(sc) {
      sc.parallelize(records, 4)
        .groupByKey()
        .collect()
    }

    assert(metrics.recordsRead === numRecords)
    assert(metrics.recordsWritten === numRecords)
/* TODO
    assert(metrics.bytesWritten === metrics.byresRead)
    assert(metrics.bytesWritten > 0)
    */
  }



}
