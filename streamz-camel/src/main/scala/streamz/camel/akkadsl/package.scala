package streamz.camel

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._

import org.apache.camel.spi.Synchronization
import org.apache.camel.{Exchange, ExchangePattern}

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util._

package object akkadsl {
  class StreamMessageScalaDsl[A, M, FO <: FlowOps[StreamMessage[A], M]](val self: FO) {
    def send(uri: String, parallelism: Int = 1)(implicit context: StreamContext): self.Repr[StreamMessage[A]] =
      self.via(akkadsl.send[A](uri, parallelism))

    def request[B](uri: String, parallelism: Int = 1)(implicit context: StreamContext, tag: ClassTag[B]): self.Repr[StreamMessage[B]] =
      self.via(akkadsl.request[A, B](uri, parallelism))
  }

  class StreamBodyScalaDsl[A, M, FO <: FlowOps[A, M]](val self: FO) {
    def send(uri: String, parallelism: Int = 1)(implicit context: StreamContext): self.Repr[A] =
      self.map(StreamMessage(_)).via(akkadsl.send[A](uri, parallelism)).map(_.body)

    def request[B](uri: String, parallelism: Int = 1)(implicit context: StreamContext, tag: ClassTag[B]): self.Repr[B] =
      self.map(StreamMessage(_)).via(akkadsl.request[A, B](uri, parallelism)).map(_.body)
  }

  implicit def streamMessageSourceScalaDsl[A, M](self: Source[StreamMessage[A], M]): StreamMessageScalaDsl[A, M, Source[StreamMessage[A], M]] =
    new StreamMessageScalaDsl(self)

  implicit def streamMessageFlowScalaDsl[A, B, M](self: Flow[A, StreamMessage[B], M]): StreamMessageScalaDsl[B, M, Flow[A, StreamMessage[B], M]] =
    new StreamMessageScalaDsl(self)

  implicit def streamMessageSourceSubFlowScalaDsl[A, M](self: SubFlow[StreamMessage[A], M, Source[StreamMessage[A], M]#Repr, Source[StreamMessage[A], M]#Closed]): StreamMessageScalaDsl[A, M, SubFlow[StreamMessage[A], M, Source[StreamMessage[A], M]#Repr, Source[StreamMessage[A], M]#Closed]] =
    new StreamMessageScalaDsl(self)

  implicit def streamMessageFlowSubFlowScalaDsl[A, B, M](self: SubFlow[StreamMessage[B], M, Flow[A, StreamMessage[B], M]#Repr, Flow[A, StreamMessage[B], M]#Closed]): StreamMessageScalaDsl[B, M, SubFlow[StreamMessage[B], M, Flow[A, StreamMessage[B], M]#Repr, Flow[A, StreamMessage[B], M]#Closed]] =
    new StreamMessageScalaDsl(self)

  implicit def streamBodySourceScalaDsl[A, M](self: Source[A, M]): StreamBodyScalaDsl[A, M, Source[A, M]] =
    new StreamBodyScalaDsl(self)

  implicit def streamBodyFlowScalaDsl[A, B, M](self: Flow[A, B, M]): StreamBodyScalaDsl[B, M, Flow[A, B, M]] =
    new StreamBodyScalaDsl(self)

  implicit def streamBodySourceSubFlowScalaDsl[B, M](self: SubFlow[B, M, Source[B, M]#Repr, Source[B, M]#Closed]): StreamBodyScalaDsl[B, M, SubFlow[B, M, Source[B, M]#Repr, Source[B, M]#Closed]] =
    new StreamBodyScalaDsl(self)

  implicit def streamBodyFlowSubFlowScalaDsl[A, B, M](self: SubFlow[B, M, Flow[A, B, M]#Repr, Flow[A, B, M]#Closed]): StreamBodyScalaDsl[B, M, SubFlow[B, M, Flow[A, B, M]#Repr, Flow[A, B, M]#Closed]] =
    new StreamBodyScalaDsl(self)

  def receiveBody[O](uri: String)(implicit streamContext: StreamContext, tag: ClassTag[O]): Source[O, NotUsed] =
    consume[O](uri).map(_.body)

  def receive[O](uri: String)(implicit streamContext: StreamContext, tag: ClassTag[O]): Source[StreamMessage[O], NotUsed] =
    consume[O](uri)

  def send[I](uri: String, parallelism: Int)(implicit context: StreamContext): Graph[FlowShape[StreamMessage[I], StreamMessage[I]], NotUsed] =
    Flow[StreamMessage[I]].mapAsync(1)(produce[I, I](uri, _, ExchangePattern.InOnly, (message, _) => message))

  def request[I, O](uri: String, parallelism: Int)(implicit context: StreamContext, tag: ClassTag[O]): Graph[FlowShape[StreamMessage[I], StreamMessage[O]], NotUsed] =
    Flow[StreamMessage[I]].mapAsync(1)(produce[I, O](uri, _, ExchangePattern.InOut, (_, exchange) => StreamMessage.from[O](exchange.getOut)))

  private def consume[O](uri: String)(implicit streamContext: StreamContext, tag: ClassTag[O]): Source[StreamMessage[O], NotUsed] =
    Source.actorPublisher[StreamMessage[O]](EndpointConsumer.props[O](uri)).mapMaterializedValue(_ => NotUsed)

  private def produce[I, O](uri: String, message: StreamMessage[I], pattern: ExchangePattern, result: (StreamMessage[I], Exchange) => StreamMessage[O])(implicit context: StreamContext): Future[StreamMessage[O]] = {
    val promise = Promise[StreamMessage[O]]()
    context.producerTemplate.asyncCallback(uri, context.exchange(message, pattern), new Synchronization {
      override def onFailure(exchange: Exchange): Unit =
        promise.failure(exchange.getException)
      override def onComplete(exchange: Exchange): Unit = Try(result(message, exchange)) match {
        case Success(r) => promise.success(result(message, exchange))
        case Failure(e) => promise.failure(e)
      }
    })
    promise.future
  }
}
