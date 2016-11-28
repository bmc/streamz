/*
 * Copyright 2014 - 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streamz.camel

import org.apache.camel._
import org.apache.camel.impl.{ DefaultExchange, DefaultCamelContext }

import scala.reflect.ClassTag

object StreamContext {
  def apply(): StreamContext =
    DefaultStreamContext.apply()

  def apply(camelContext: CamelContext): StreamContext =
    new StreamContext(camelContext).start()
}

class StreamContext(val camelContext: CamelContext) {
  lazy val consumerTemplate: ConsumerTemplate =
    camelContext.createConsumerTemplate()

  lazy val producerTemplate: ProducerTemplate =
    camelContext.createProducerTemplate()

  def exchange[A](message: StreamMessage[A], pattern: ExchangePattern): Exchange = {
    val exchange = new DefaultExchange(camelContext, pattern)
    exchange.setIn(message.camelMessage)
    exchange
  }

  def convert[A](obj: Any)(implicit classTag: ClassTag[A]): A = {
    val clazz = classTag.runtimeClass.asInstanceOf[Class[A]]
    val result = camelContext.getTypeConverter.mandatoryConvertTo[A](clazz, obj)

    obj match {
      case cache: StreamCache => cache.reset()
      case _ =>
    }

    result
  }

  def start(): StreamContext = {
    consumerTemplate.start()
    producerTemplate.start()
    this
  }

  def stop(): StreamContext = {
    consumerTemplate.stop()
    producerTemplate.stop()
    this
  }
}

object DefaultStreamContext {
  def apply(): DefaultStreamContext =
    new DefaultStreamContext().start()
}

class DefaultStreamContext extends StreamContext(new DefaultCamelContext) {
  override def start(): DefaultStreamContext = {
    camelContext.start()
    super.start()
    this
  }

  override def stop(): DefaultStreamContext = {
    super.stop()
    camelContext.stop()
    this
  }
}
