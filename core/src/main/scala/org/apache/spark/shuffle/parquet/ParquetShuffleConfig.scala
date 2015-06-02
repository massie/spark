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

import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.{SparkConf, SparkEnv}

object ParquetShuffleConfig {
  val SPARK_SHUFFLE_MANAGER_KEY = "spark.shuffle.manager"
  val MANAGER_SHORTNAME = "parquet"
  val NAMESPACE = "spark.shuffle.parquet."
  val COMPRESSION_KEY = NAMESPACE + "compression"
  val BLOCKSIZE_KEY = NAMESPACE + "blocksize"
  val PAGESIZE_KEY = NAMESPACE + "pagesize"
  val ENABLEDICTIONARY_KEY = NAMESPACE + "enabledictionary"
  val REGISTRATOR_KEY = NAMESPACE + "registrator"
  val FALLBACK_SHUFFLE_MANAGER = NAMESPACE + "fallback"

  def isParquetShuffleEnabled: Boolean = {
    isParquetShuffleEnabled(SparkEnv.get.conf)
  }

  def isParquetShuffleEnabled(conf: SparkConf): Boolean = {
    val confValue = conf.get(SPARK_SHUFFLE_MANAGER_KEY, "")
    confValue == MANAGER_SHORTNAME || confValue == classOf[ParquetShuffleManager].getName
  }

  def getCompression: CompressionCodecName = {
    getCompression(SparkEnv.get.conf)
  }

  def getCompression(conf: SparkConf): CompressionCodecName = {
    val confValue = conf.get(COMPRESSION_KEY, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME
    } else {
      CompressionCodecName.fromConf(confValue)
    }
  }

  def getBlockSize: Int = {
    getBlockSize(SparkEnv.get.conf)
  }

  def getBlockSize(conf: SparkConf): Int = {
    val confValue = conf.get(BLOCKSIZE_KEY, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_BLOCK_SIZE
    } else {
      confValue.toInt
    }
  }

  def getPageSize: Int = {
    getPageSize(SparkEnv.get.conf)
  }

  def getPageSize(conf: SparkConf): Int = {
    val confValue = conf.get(PAGESIZE_KEY, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_PAGE_SIZE
    } else {
      confValue.toInt
    }
  }

  def isDictionaryEnabled: Boolean = {
    isDictionaryEnabled(SparkEnv.get.conf)
  }

  def isDictionaryEnabled(conf: SparkConf): Boolean = {
    val confValue = conf.get(ENABLEDICTIONARY_KEY, null)
    if (confValue == null) {
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED
    } else {
      confValue.toBoolean
    }
  }

  def getFallbackShuffleManager: ShuffleManager = {
    getFallbackShuffleManager(SparkEnv.get.conf)
  }

  def getFallbackShuffleManager(conf: SparkConf): ShuffleManager = {
    val confValue = conf.get(FALLBACK_SHUFFLE_MANAGER, null)
    if (confValue == null) {
      // Default is the sort shuffle manager
      new SortShuffleManager(conf)
    } else {
      Class.forName(confValue).newInstance().asInstanceOf[ShuffleManager]
    }
  }

}
