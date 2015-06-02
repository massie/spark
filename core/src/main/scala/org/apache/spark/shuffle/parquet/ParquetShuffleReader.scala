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

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader

import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.hash.BlockStoreShuffleFetcher
import org.apache.spark.shuffle.parquet.avro.AvroPair

import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark._

class ParquetShuffleReader[K, V, C](
    handle: ParquetShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {
  require(endPartition == startPartition + 1,
    "Parquet shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val shuffleId = handle.shuffleId
  private val reduceId = startPartition

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val blockStreams = BlockStoreShuffleFetcher.fetchBlockStreams(
      handle.shuffleId, startPartition, context, blockManager, mapOutputTracker)

    val readMetrics = context.taskMetrics().createShuffleReadMetricsForDependency()

    val recordIterator = CompletionIterator[Product2[Any, Any],
      Iterator[Product2[Any, Any]]](
        for ((blockId, inputStream) <- blockStreams;
             record <- {
               // Parquet needs to work with Files instead of InputStreams, so we
               // (1) Request a local, temporary block for the remote data
               val (tBlockId, tBlock) = blockManager.diskBlockManager.createTempLocalBlock()
               // (2) Copy all data from the InputStream to the local, temporary block File
               Files.copy(inputStream, tBlock.toPath)
               // (3) Close the InputStream, and
               inputStream.close()
               // (4) Load the Parquet data from the temporary block File
               val reader = new AvroParquetReader[AvroPair[Any, Any]](
                 new Path(tBlock.getCanonicalPath))
               val iterator = Iterator.continually(reader.read()).takeWhile(_ != null)
               CompletionIterator[Product2[Any, Any], Iterator[Product2[Any, Any]]](iterator, {
                 // Once all the records are read, close the reader and input stream as soon as
                 // possible, to allow a release of the underlying ManagedBuffer memory
                 reader.close()
                 inputStream.close()
               })
             }) yield {
          // Update the read metrics for each record that is read
          readMetrics.incRecordsRead(1)
          record
        },
        // When the iterator completes, update all the shuffle metrics
        context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[Product2[Any, Any]](context, recordIterator)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        // TODO: revisit this. Serializer will be record-oriented which isn't ideal.
        val ser = Serializer.getSerializer(dep.serializer)
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics.incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics.incDiskBytesSpilled(sorter.diskBytesSpilled)
        sorter.iterator
      case None =>
        aggregatedIter
    }
  }
}
