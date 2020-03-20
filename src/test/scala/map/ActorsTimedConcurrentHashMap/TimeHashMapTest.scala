package map.ActorsTimedConcurrentHashMap

import java.util.concurrent.{TimeUnit}

import org.scalatest.concurrent.ConductorMethods
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.ScalaFutures._
import scala.concurrent.duration.FiniteDuration

class TimeHashMapTest extends FunSuite with BeforeAndAfter with ConductorMethods {

  var map: TimeHashMap[Long, Any] = _
  before {
    map = new TimeHashMap[Long, Any](ActorSystemRef.system)
  }

  test("testTimelyRemove") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
    Thread.sleep(1500)
    whenReady(map.size()) { size =>
      assert(size == 0)
    }
  }

  test("testRemove") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.remove(1)
    whenReady(map.size()) { size =>
      assert(size == 0)
    }
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.put(2, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.remove(1)
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
  }

  test("testSize") {
    whenReady(map.size()) { size =>
      assert(size == 0)
    }
    map.put(1, 1, FiniteDuration(500, TimeUnit.SECONDS))
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
  }

  test("testIRI") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
    map.remove(1)
    whenReady(map.size()) { size =>
      assert(size == 0)
    }
    map.put(1, 1, FiniteDuration(3, TimeUnit.SECONDS))
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
    Thread.sleep(2000)
    whenReady(map.size()) { size =>
      assert(size == 1)
    }
    Thread.sleep(2000)
    whenReady(map.size()) { size =>
      assert(size == 0)
    }
  }

  test("should return last value") {
    map.put(10, 10, FiniteDuration(3, TimeUnit.SECONDS))
    map.put(10, 11, FiniteDuration(5, TimeUnit.SECONDS))
    whenReady(map.get(10)) {
      optValue =>
        assert(optValue == Some(11))
    }

    whenReady(map.size()) {
      size => assert(size == 1)
    }
  }

  test("parallel Put") {

    threadNamed("t1") {
      for (i <- 1 to 10) yield {
        map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      for (i <- 11 to 20) yield {
        map.put(i, i, FiniteDuration(2, TimeUnit.SECONDS))
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 20)
      }
      Thread.sleep(2000 * 10 + 1000 * 10)
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }

  test("parallel put but only 10 left") {

    threadNamed("t1") {
      for (i <- 1 to 10) yield {
        map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      for (i <- 1 to 10) yield {
        map.put(i, Some(i), FiniteDuration(2, TimeUnit.SECONDS))
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 10)
      }
      Thread.sleep(2000 * 10)
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }

  test("put and remove") {

    threadNamed("t1") {
      for (i <- 1 to 3) yield {
        map.put(i, i, FiniteDuration(3, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      waitForBeat(1)
      for (i <- 1 to 3) yield {
        whenReady(map.remove(i)) { value =>
          assert(value == Some(i))
        }
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }

  test("parallel remove") {
    threadNamed("t1") {
      for (i <- 1 to 3) yield {
        map.put(i, Some(i), FiniteDuration(3, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      waitForBeat(1)
      for (i <- 1 to 3) yield {
        map.remove(i)
      }
    }

    threadNamed("t3") {
      waitForBeat(1)
      for (i <- 1 to 3) yield {
        map.remove(i)
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }

  test("parallel multiple put and remove- remove can happen before put") {
    threadNamed("t1") {
      for (i <- 1 to 3) yield {
        map.put(i, Some(i), FiniteDuration(3, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      for (i <- 1 to 3) yield {
        map.remove(i)
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }


  test("parallel single put and remove- remove can happen before put") {
    whenReady(map.size()) {
      size => assert(size == 0)
    }

    threadNamed("t1") {
      map.put(23, 23, FiniteDuration(3, TimeUnit.SECONDS))

    }

    threadNamed("t2") {
      whenReady(map.remove(23)){
        value=>println(value)
      }
    }

    whenFinished {
      whenReady(map.size()) { size =>
        assert(size == 0)
      }
    }
  }

}