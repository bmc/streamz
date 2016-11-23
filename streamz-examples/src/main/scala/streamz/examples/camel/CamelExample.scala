package streamz.examples.camel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import fs2.{Strategy, Stream, Task}

import streamz.camel.StreamContext

import scala.concurrent.ExecutionContext.Implicits.global

object CamelFs2Example {
  import streamz.camel.fs2dsl._

  implicit val context = StreamContext()
  implicit val strategy = Strategy.fromExecutionContext(global)

  val s: Stream[Task, Int] =
    // receive from endpoint
    receiveBody[String]("seda:q1")
    // in-only message exchange with endpoint and continue stream with in-message
    .send("seda:q2")
    // in-out message exchange with endpoint and continue stream with out-message
    .request[Int]("bean:service?method=length")
    // in-only message exchange with endpoint
    .send("seda:q3")

  // create concurrent task from stream
  val t: Task[Unit] = s.run

  // run task (side effects only here) ...
  t.unsafeRun

  val s1: Stream[Task, String] = receiveBody[String]("seda:q1")
  val s2: Stream[Task, String] = s1.send("seda:q2")
  val s3: Stream[Task, Int] = s1.request[Int]("bean:service?method=length")
}

object CamelAkkaExample extends App {
  import streamz.camel.akkadsl._

  implicit val system = ActorSystem("example")
  implicit val materializer = ActorMaterializer()
  implicit val context = StreamContext()

  val s: Source[Int, NotUsed] =
    // receive from endpoint
    receiveBody[String]("seda:q1")
    // in-only message exchange with endpoint and continue stream with in-message
    .send("seda:q2")
    // in-out message exchange with endpoint and continue stream with out-message
    .request[Int]("bean:service?method=length")
    // in-only message exchange with endpoint
    .send("seda:q3")

  // run stream (side effects only here) ...
  s.runForeach(println)
}
