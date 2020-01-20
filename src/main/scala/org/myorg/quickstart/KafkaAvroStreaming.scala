/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization._
import org.apache.flink.streaming.connectors.kafka._
import java.util._

import id.dei.PageViews
import org.apache.flink.api.common.typeinfo.TypeInformation

class PageViewsSerializationSchema extends DeserializationSchema[PageViews] {
  override def deserialize(bytes: Array[Byte]): PageViews = {
    PageViews.getDecoder.decode(bytes)
  }

  override def isEndOfStream(t: PageViews): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[PageViews] = {
    TypeInformation.of(classOf[PageViews])
  }
}
object KafkaAvroStreaming {
  def main(args: Array[String])
  {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "34.87.113.63:9092")
    properties.setProperty("group.id", "test")

    val stream = env
        .addSource(new FlinkKafkaConsumer[PageViews]("telegram", new PageViewsSerializationSchema(), properties))
        .print()

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
