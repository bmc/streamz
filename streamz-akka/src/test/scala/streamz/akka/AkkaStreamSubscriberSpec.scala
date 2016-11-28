package streamz.akka

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorSubscriberMessage._
import akka.stream.scaladsl._
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit._

import org.scalatest._

object AkkaStreamSubscriberSpec {
  class TestAkkaStreamSubscriber(probe: TestProbe) extends AkkaStreamSubscriber[Int] {
    override def waiting: Receive = {
      case m: OnError =>
        super.waiting(m)
        probe.ref ! m
      case m @ OnComplete =>
        super.waiting(m)
        probe.ref ! m
      case m if super.waiting.isDefinedAt(m) =>
        super.waiting(m)
    }
  }
}

class AkkaStreamSubscriberSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll {
  import AkkaStreamSubscriberSpec._
  import AkkaStreamSubscriber._

  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  override def afterAll(): Unit = {
    materializer.shutdown()
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def callback(probe: TestProbe): Converter.Callback[Option[Int]] =
    probe.ref ! _

  def testSourceAndActorSubscriber(probe: TestProbe = TestProbe()): (TestPublisher.Probe[Int], ActorRef) =
    TestSource.probe[Int].toMat(Sink.actorSubscriber(Props(new TestAkkaStreamSubscriber(probe))))(Keep.both).run()

  "An AkkaStreamSubscriber" when {
    "in waiting state" must {
      "forward demand to upstream if stream is active" in {
        val (src, snk) = testSourceAndActorSubscriber()

        snk ! Request(callback(TestProbe()))
        src.expectRequest() should be(1L)
      }
      "call back with an error if the stream failed" in {
        val probe = TestProbe()
        val (src, snk) = testSourceAndActorSubscriber(probe)

        src.sendError(ConverterSpec.error)
        probe.expectMsg(OnError(ConverterSpec.error))
        snk ! Request(callback(probe))
        probe.expectMsg(Left(ConverterSpec.error))
      }
      "call back with an undefined value if the stream completed" in {
        val probe = TestProbe()
        val (src, snk) = testSourceAndActorSubscriber(probe)

        src.sendComplete()
        probe.expectMsg(OnComplete)
        snk ! Request(callback(probe))
        probe.expectMsg(Right(None))
      }
    }
    "in requesting state" must {
      "call back with an element if element was received" in {
        val probe = TestProbe()
        val (src, snk) = testSourceAndActorSubscriber(probe)

        snk ! Request(callback(probe))
        src.expectRequest()
        src.sendNext(1)
        probe.expectMsg(Right(Some(1)))
      }
      "call back with an undefined value if completion was received" in {
        val probe = TestProbe()
        val (src, snk) = testSourceAndActorSubscriber(probe)

        snk ! Request(callback(probe))
        src.expectRequest()
        src.sendComplete()
        probe.expectMsg(Right(None))
      }
      "call back with an error if error was received" in {
        val probe = TestProbe()
        val (src, snk) = testSourceAndActorSubscriber(probe)

        snk ! Request(callback(probe))
        src.expectRequest()
        src.sendError(ConverterSpec.error)
        probe.expectMsg(Left(ConverterSpec.error))
      }
    }
  }
}
