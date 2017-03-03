/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.algebird

import com.twitter.algebird.CMSHasher
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.state.internals.AbstractStoreSupplier

/**
  * A factory for Kafka Streams to instantiate a [[CMSStore]].
  */
class CMSStoreSupplier[T: CMSHasher](name: String,
                                     val serde: Serde[T],
                                     time: Time,
                                     logged: Boolean,
                                     logConfig: java.util.Map[String, String])
    extends AbstractStoreSupplier[T, Long, CMSStore[T]](name, serde, Serdes.Long().asInstanceOf[Serde[Long]], time, logged, logConfig) {

  def this(name: String, serde: Serde[T]) {
    this(name, serde, null, true, new java.util.HashMap[String, String])
  }

  def this(name: String, serde: Serde[T], logged: Boolean) {
    this(name, serde, null, logged, new java.util.HashMap[String, String])
  }

  def this(name: String, serde: Serde[T], logged: Boolean, logConfig: java.util.Map[String, String]) {
    this(name, serde, null, logged, logConfig)
  }

  override def get(): CMSStore[T] = new CMSStore[T](name, serde)

}