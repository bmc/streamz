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

import java.io.InputStream

import fs2._

import streamz.camel.StreamContext
import streamz.camel.fs2dsl._

import scala.concurrent.ExecutionContext.Implicits.global

object CamelFtpExample extends App {
  implicit val context = StreamContext()
  implicit val strategy = Strategy.fromExecutionContext(global)

  // FTP server endpoint accessed via camel-ftp (see also
  // http://camel.apache.org/components.html for a complete
  // list of configurable endpoints).
  val enpointUri = "ftp://ftp.example.com?antInclude=*.txt&idempotent=true"

  val ftpLines: Stream[Task, String] = for {
    // receive existing (and new) *.txt files from server
    is <- receiveBody[InputStream](enpointUri)
    // split each file into lines
    line <- Stream.repeatEval(Task.delay(is.read()))
      .takeWhile(_ != -1)
      .map(_.toByte)
      .through(text.utf8Decode)
      .through(text.lines)
  } yield line

  val printUpper: Stream[Task, Unit] = ftpLines
    // convert lines to upper case
    .map(_.toUpperCase)
    // write lines from all files to stdout
    .map(println)

  // side effects here ...
  printUpper.run.unsafeRun

  // To process files from a local directory, change the enpointUri to
  // "file:testdata?noop=true". After having started the process, add new text
  // files to the testdata directory and they will be automatically processed.
}
