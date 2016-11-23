package streamz.camel

import fs2._

import org.apache.camel.spi.Synchronization
import org.apache.camel.{Exchange, ExchangePattern}

import scala.reflect.ClassTag
import scala.util._

package object fs2dsl {
  implicit class StreamMessageDsl[A](self: Stream[Task, StreamMessage[A]]) {
    def send(uri: String)(implicit context: StreamContext, strategy: Strategy): Stream[Task, StreamMessage[A]] =
      self.through(fs2dsl.send[A](uri))

    def request[B](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[B]): Stream[Task, StreamMessage[B]] =
      self.through(fs2dsl.request[A, B](uri))
  }

  implicit class StreamBodyDsl[A](self: Stream[Task, A]) {
    def send(uri: String)(implicit context: StreamContext, strategy: Strategy): Stream[Task, A] =
      self.map(StreamMessage(_)).send(uri).map(_.body)

    def request[B](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[B]): Stream[Task, B] =
      self.map(StreamMessage(_)).request[B](uri).map(_.body)
  }

  /**
    * Produces a discrete stream of message bodies received at the Camel endpoint identified by `uri`.
    * If needed, received message bodies are converted to type `O` using a Camel type converter.
    *
    * @param uri Camel endpoint URI.
    */
  def receiveBody[O](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[O]): Stream[Task, O] =
    receive(uri).map(_.body)

  /**
    * ...
    *
    * @param uri Camel endpoint URI.
    */
  def receive[O](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[O]): Stream[Task, StreamMessage[O]] = {
    consume(uri).filter(_ != null)
  }

  /**
    * A pipe that initiates an in-only message exchange with the Camel endpoint identified by `uri`.
    * Continues the stream with the input message.
    *
    * @param uri Camel endpoint URI.
    */
  def send[I](uri: String)(implicit context: StreamContext, strategy: Strategy): Pipe[Task, StreamMessage[I], StreamMessage[I]] =
    produce[I, I](uri, ExchangePattern.InOnly, (message, _) => message)

  /**
    * A pipe that initiates an in-out message exchange with the Camel endpoint identified by `uri`.
    * Continues the stream with the output message. If needed, received output message bodies are
    * converted to type `O` using a Camel type converter.
    *
    * @param uri Camel endpoint URI.
    */
  def request[I, O](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[O]): Pipe[Task, StreamMessage[I], StreamMessage[O]] =
    produce[I, O](uri, ExchangePattern.InOut, (_, exchange) => StreamMessage.from[O](exchange.getOut))

  private def consume[O](uri: String)(implicit context: StreamContext, strategy: Strategy, tag: ClassTag[O]): Stream[Task, StreamMessage[O]] = {
    import context._
    Stream.repeatEval {
      Task.async[StreamMessage[O]] { callback =>
        Try(consumerTemplate.receive(uri, 500)) match {
          case Success(null) =>
            callback(Right(null))
          case Success(ce) if ce.getException != null =>
            callback(Left(ce.getException))
            consumerTemplate.doneUoW(ce)
          case Success(ce) =>
            Try(StreamMessage.from[O](ce.getIn)) match {
              case Success(m) => callback(Right(m))
              case Failure(e) => callback(Left(e))
            }
            consumerTemplate.doneUoW(ce)
          case Failure(ex) =>
            callback(Left(ex))
        }
      }
    }
  }

  private def produce[I, O](uri: String, pattern: ExchangePattern, result: (StreamMessage[I], Exchange) => StreamMessage[O])(implicit context: StreamContext, strategy: Strategy): Pipe[Task, StreamMessage[I], StreamMessage[O]] = { s =>
    import context._
    s.flatMap { message =>
      Stream.eval {
        Task.async[StreamMessage[O]] { callback =>
          producerTemplate.asyncCallback(uri, context.exchange(message, pattern), new Synchronization {
            override def onFailure(exchange: Exchange): Unit =
              callback(Left(exchange.getException))
            override def onComplete(exchange: Exchange): Unit = Try(result(message, exchange)) match {
              case Success(r) => callback(Right(result(message, exchange)))
              case Failure(e) => callback(Left(e))
            }
          })
        }
      }
    }
  }
}
