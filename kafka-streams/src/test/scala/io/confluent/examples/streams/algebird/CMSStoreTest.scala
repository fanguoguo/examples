/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.examples.streams.algebird

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.mockito.Mockito
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar

class CMSStoreTest extends AssertionsForJUnit with MockitoSugar {

  private val anyStoreName = "cms-store"
  private val anySerde = Serdes.String()

  private def initializedStore(): CMSStore[String] = {
    val store = new CMSStore(anyStoreName, anySerde)
    val processorContext = mock[ProcessorContext](Mockito.withSettings().stubOnly())
    val root = mock[StateStore](Mockito.withSettings().stubOnly())
    store.init(processorContext, root)
    store
  }

  @Test
  def shouldBeNonPersistentStore(): Unit = {
    val store = new CMSStore(anyStoreName, anySerde)
    assertThat(store.persistent).isFalse
  }

  @Test
  def shouldBeClosedBeforeInit(): Unit = {
    val store = new CMSStore(anyStoreName, anySerde)
    assertThat(store.isOpen).isFalse
  }

  @Test
  def shouldBeOpenAndHaveZeroCountsAfterInit(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore(anyStoreName, anySerde)

    // When
    val processorContext = mock[ProcessorContext](Mockito.withSettings().stubOnly())
    val root = mock[StateStore](Mockito.withSettings().stubOnly())
    store.init(processorContext, root)

    // Then
    assertThat(store.isOpen).isTrue
    assertThat(store.totalCount).isZero
    assertThat(store.heavyHitters.size).isZero
  }

  @Test
  def shouldBeClosedAfterClosing(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore(anyStoreName, anySerde)

    // When
    store.close()

    // Then
    assertThat(store.isOpen).isFalse
  }

  @Test
  def shouldExactlyCountSmallNumbersOfItems(): Unit = {
    // Given
    val store: CMSStore[String] = initializedStore()

    // When
    val input = Seq("foo", "bar", "quux", "foo", "foo", "foo", "quux", "bar", "foo")
    input.foreach(store.put)

    // Then
    assertThat(store.totalCount).isEqualTo(input.size)
    assertThat(store.heavyHitters).isEqualTo(input.toSet)
    // Note: For large amounts of input data we wouldn't expect counts to be exact any longer.
    val expWordCounts: Map[String, Int] = input.groupBy(identity).mapValues(_.length)
    expWordCounts.foreach { case (word, count) => assertThat(store.get(word)).isEqualTo(count) }
  }

}