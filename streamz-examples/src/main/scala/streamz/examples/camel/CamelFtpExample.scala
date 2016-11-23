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
  val enpointUri ="ftp://ftp.example.com?antInclude=*.txt&idempotent=true"

  val ftpLines: Stream[Task, String] = for {
    // receive existing (and new) *.txt files from server
    is  <- receiveBody[InputStream](enpointUri)
    // split each file into lines
    line <- Stream.repeatEval(Task.delay(is.read()))
      .takeWhile(_ != -1)
      .map(_.toByte)
      .through(text.utf8Decode)
      .through(text.lines)
  } yield line

  val printUpper: Stream[Task,Unit] = ftpLines
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
