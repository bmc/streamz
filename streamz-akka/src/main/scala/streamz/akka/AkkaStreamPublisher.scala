package streamz.akka

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._

import streamz.akka.Converter.Callback

private[akka] object AkkaStreamPublisher {
  case class Next[A](elem: A, cb: Callback[Option[Unit]])
  case class Error(cause: Throwable)
  case object Complete

  def props[A]: Props =
    Props(new AkkaStreamPublisher[A])
}

private[akka] class AkkaStreamPublisher[A] extends ActorPublisher[A] {
  import AkkaStreamPublisher._

  private val defined = Some(())
  private var next: Option[Next[A]] = None

  override def receive = {
    //
    // Messages from upstream (fs2)
    //
    case n: Next[A] if isCanceled =>
      n.cb(Right(None))
    case n: Next[A] if totalDemand > 0 =>
      assert(next.isEmpty)
      onNext(n.elem)
      n.cb(Right(defined))
    case n: Next[A] =>
      next = Some(n)
    case Error(cause) =>
      onError(cause)
    case Complete if !isErrorEmitted =>
      onComplete()
    //
    // Messages from downstream
    //
    case r: Request =>
      next.foreach { n =>
        onNext(n.elem)
        n.cb(Right(Some(())))
        next = None
      }
  }
}
