package streamz.camel

import org.apache.camel.impl.DefaultMessage
import org.apache.camel.{Message => CamelMessage}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

case class StreamMessage[A](body: A, headers: Map[String, Any] = Map.empty) {
  def bodyAs[B](implicit tag: ClassTag[B], streamContext: StreamContext): B =
    streamContext.convert(body)

  def headerAs[B](name: String)(implicit tag: ClassTag[B], streamContext: StreamContext): B =
    headerOptionAs[B](name).get

  def headerOptionAs[B](name: String)(implicit tag: ClassTag[B], streamContext: StreamContext): Option[B] =
    headers.get(name).map(streamContext.convert)

  private[camel] def camelMessage: CamelMessage = {
    val result = new DefaultMessage

    headers.foreach {
      case (k, v) => result.setHeader(k, v)
    }

    result.setBody(body)
    result
  }
}

object StreamMessage {
  def from[A](camelMessage: CamelMessage)(implicit tag: ClassTag[A]): StreamMessage[A] =
    new StreamMessage(camelMessage.getBody(tag.runtimeClass.asInstanceOf[Class[A]]), camelMessage.getHeaders.asScala.toMap)
}
