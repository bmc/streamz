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

package streamz.examples.camel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._

import fs2.{ Strategy, Stream, Task }

import streamz.camel.StreamContext

import scala.concurrent.ExecutionContext.global

object CamelFs2Snippets {
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

  // create task from stream
  val t: Task[Unit] = s.run

  // run task (side effects only here) ...
  t.unsafeRun

  val s1: Stream[Task, String] = receiveBody[String]("seda:q1")
  val s2: Stream[Task, String] = s1.send("seda:q2")
  val s3: Stream[Task, Int] = s1.request[Int]("bean:service?method=length")
}

object CamelAkkaSnippets extends App {
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
