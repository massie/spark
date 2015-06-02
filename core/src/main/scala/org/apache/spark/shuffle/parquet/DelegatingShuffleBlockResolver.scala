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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId

/**
 * Retrieves block data based from either the parquet block resolver or the
 * fallback shuffle manager's block resolver based on the block requested
 * @param parquetShuffleManager manages blockIds and the fallback manager
 */
class DelegatingShuffleBlockResolver(parquetShuffleManager: ParquetShuffleManager)
  extends ShuffleBlockResolver {

  private def isParquetShuffleBlock(blockId: ShuffleBlockId): Boolean = {
    !parquetShuffleManager.fallbackShuffleIds.contains(blockId.shuffleId)
  }

  /**
   * Retrieve the data for the specified block. If the data for that block is not available,
   * throws an unspecified exception.
   */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    if (isParquetShuffleBlock(blockId)) {
      parquetShuffleManager.fileShuffleBlockManager.getBlockData(blockId)
    } else {
      parquetShuffleManager.fallbackManager.shuffleBlockResolver.getBlockData(blockId)
    }
  }

  override def stop(): Unit = {
    // Stop the parquet shuffle block resolver
    parquetShuffleManager.fileShuffleBlockManager.stop()
    // Stop the fallback shuffle block resolver
    parquetShuffleManager.fallbackManager.shuffleBlockResolver.stop()
  }
}
