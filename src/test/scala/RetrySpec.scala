import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import common.Futures
import org.specs2.mutable.{Specification, Tags}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import server.implicits._

class RetrySpec extends Specification with Tags {
  sequential

  val counter = new AtomicInteger(0)
  val counter2 = new AtomicInteger(0)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  def fuuu = {
    counter.incrementAndGet()
    Future.failed(new RuntimeException("ERROR!!!"))
  }
  def fuuu2 = {
    counter2.incrementAndGet()
    if (counter2.get() == 3) Future.successful("Yeahhhh !!!")
    else Future.failed(new RuntimeException("ERROR!!!"))
  }

  "Retry API" should {

    "Retry stuff 3 times" in {
      val f = Futures.retry(3)(fuuu)
      f.onComplete {
        case Success(s) => println(s.toString)
        case Failure(e) => println(e.getMessage)
      }
      Thread.sleep(2000)
      counter.get() shouldEqual 3
      success
    }

    "Retry stuff 3 times" in {
      val f = Futures.retry(5)(fuuu2)
      f.onComplete {
        case Success(s) => println(s.toString)
        case Failure(e) => println(e.getMessage)
      }
      Thread.sleep(2000)
      counter.get() shouldEqual 3
      success
    }

  }
}