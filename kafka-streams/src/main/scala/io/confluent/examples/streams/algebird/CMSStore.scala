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

import com.twitter.algebird.{CMSHasher, TopCMS, TopPctCMS}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.StateSerdes

/**
  * An in-memory store that leverages the Count-Min Sketch implementation of
  * [[https://github.com/twitter/algebird Twitter Algebird]].
  *
  * This store allows you to probabilistically count items of type T.  Here, the counts returned
  * by the store will be approximate counts, i.e. estimations, because a Count-Min Sketch trades
  * greatly reduced space utilization for slightly inaccurate counts (however, the estimation error
  * is mathematically proven to be bounded).
  *
  *
  * =Usage=
  *
  * In a Kafka Streams application, you'd typically create this store as such:
  *
  * {{{
  * val builder: KStreamBuilder = new KStreamBuilder()
  *
  * // In this example, we create a store for type [[String]].
  * builder.addStateStore(new CMSStoreSupplier[String]("my-cms-store-name", Serdes.String()))
  * }}}
  *
  * Then you'd use the store within a [[org.apache.kafka.streams.processor.Processor]] or a
  * [[org.apache.kafka.streams.kstream.Transformer]] similar to:
  *
  * {{{
  * class ProbabilisticCounter extends Transformer[Array[Byte], String, KeyValue[String, Long]] {
  *
  *   private var cmsState: CMSStore[String] = _
  *   private var processorContext: ProcessorContext = _
  *
  *   override def init(processorContext: ProcessorContext): Unit = {
  *     this.processorContext = processorContext
  *     cmsState = this.processorContext.getStateStore(cmsStoreName).asInstanceOf[CMSStore[String]]
  *   }
  *
  *   override def transform(key: Array[Byte], value: String): KeyValue[String, Long] = {
  *     // Count the record value, think: "+ 1"
  *     cmsState.put(value)
  *
  *     // Emit the latest count estimate for the record value
  *     KeyValue.pair[String, Long](value, cmsState.get(value))
  *   }
  *
  *   override def punctuate(l: Long): KeyValue[String, Long] = null
  *
  *   override def close(): Unit = {}
  * }
  * }}}
  */
class CMSStore[T: CMSHasher](val name: String, val serde: Serde[T]) extends StateStore {

  // TODO: Support changelogging via `StoreChangeLogger` (cf. Kafka's `InMemoryKeyValueLoggedStore`) to allow for fault-tolerant state

  // TODO: Make the CMS parameters configurable
  private val cmsMonoid = {
    val delta = 1E-10
    val eps = 0.001
    val seed = 1
    val heavyHittersPct = 0.01
    TopPctCMS.monoid[T](eps, delta, seed, heavyHittersPct)
  }

  private var cms: TopCMS[T] = _

  @volatile private var open: Boolean = false

  private var serdes: StateSerdes[T, Long] = _

  def init(context: ProcessorContext, root: StateStore) {
    this.serdes = new StateSerdes[T, Long](
      name,
      if (serde == null) context.keySerde.asInstanceOf[Serde[T]] else serde,
      Serdes.Long().asInstanceOf[Serde[Long]])

    cms = cmsMonoid.zero
    context.register(root, true, (key, value) => {
      // TODO: Implement the state store restoration function (which would restore the `cms` instance)
    })

    open = true
  }

  /**
    * Returns the estimated count of the item.
    * @param item item to be counted
    * @return estimated count
    */
  def get(item: T): Long = cms.frequency(item).estimate

  /**
    * Counts the item.
    * @param item item to be counted
    */
  def put(item: T): Unit = cms = cms + item

  /**
    * The top items counted so far, with the percentage-based cut-off being defined by the CMS
    * parameter `heavyHittersPct` (currently hardcoded to `0.01` and not user-configurable).
    * @return the top items counted so far
    */
  def heavyHitters: Set[T] = cms.heavyHitters

  /**
    * Returns the number of count operations performed so far.
    *
    * This number is not the same as the total number of <em>unique</em> items counted so far, i.e.
    * it is not the cardinality of the set of items.
    *
    * @return number of count operations so far
    */
  def totalCount: Long = cms.totalCount

  override val persistent: Boolean = false

  override def isOpen: Boolean = open

  override def flush() {
    // do nothing because this store is in-memory
  }

  override def close() {
    open = false
  }

}