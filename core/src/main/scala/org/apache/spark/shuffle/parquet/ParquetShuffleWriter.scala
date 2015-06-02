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

import java.io.File

import scala.util.{Failure, Success, Try}

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.parquet.avro.AvroPair
import org.apache.spark.shuffle.{FileShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.{Logging, SparkEnv, TaskContext}

case class AvroFileWriter[K](file: File, writer: AvroParquetWriter[AvroPair[K, Any]])

class ParquetShuffleWriter[K, V](shuffleBlockResolver: FileShuffleBlockResolver,
                                 handle: ParquetShuffleHandle[K, V, _],
                                 mapId: Int,
                                 context: TaskContext) extends ShuffleWriter[K, V] with Logging {
  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val blockManager = SparkEnv.get.blockManager
  private var stopping = false

  private val metrics = context.taskMetrics()
  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val avroSchema = AvroPair.makePairSchema(
    new Schema.Parser().parse(handle.keySchema.get),
    new Schema.Parser().parse(handle.valueSchema.get))

  private val ser = Serializer.getSerializer(dep.serializer.orNull)
  private val writers = Array.tabulate[AvroFileWriter[K]](numOutputSplits) {
    bucketId =>
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, bucketId)
      val outputFile = blockManager.diskBlockManager.getFile(blockId)
      val outputPath = new Path(outputFile.getCanonicalPath)
      AvroFileWriter(outputFile,
        new AvroParquetWriter[AvroPair[K, Any]](outputPath,
        avroSchema, ParquetShuffleConfig.getCompression, ParquetShuffleConfig.getBlockSize,
        ParquetShuffleConfig.getPageSize, ParquetShuffleConfig.isDictionaryEnabled))
  }

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    val iter = dep.aggregator match {
      case Some(aggregator) =>
        if(dep.mapSideCombine) {
          aggregator.combineValuesByKey(records, context)
        } else {
          records
        }
      case None =>
        require(!dep.mapSideCombine, "Map-size combine requested without an aggregator specified")
        records
    }

    for (elem <- iter) {
      val bucketId = dep.partitioner.getPartition(elem._1)
      writers(bucketId).writer.write(
        new AvroPair[K, Any](elem._1, elem._2, avroSchema))
      writeMetrics.incShuffleRecordsWritten(1)
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    stopping match {
      case true => None
      case false =>
        stopping = true
        val status = Try(writers.map { avro: AvroFileWriter[K] =>
          avro.writer.close()
          avro.file.length()
        })
        status match {
          case Success(sizes) =>
            Some(MapStatus(blockManager.shuffleServerId, sizes))
          case f: Failure[Array[Long]] =>
            throw f.exception
        }
    }
  }
}
