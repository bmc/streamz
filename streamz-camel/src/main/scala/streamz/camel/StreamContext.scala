package streamz.camel

import org.apache.camel._
import org.apache.camel.impl.{DefaultExchange, DefaultCamelContext}

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
      case _                  =>
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
