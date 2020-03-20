package map.TimedConcurrentHashMap

import java.util.concurrent.TimeUnit

import map.ActorsTimedConcurrentHashMap.ActorSystemRef
import org.scalatest.concurrent.{ConductorMethods, Conductors}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.duration.FiniteDuration

class TimeHashMapConcurrentTest extends FunSuite with BeforeAndAfter with ConductorMethods with Matchers {

  var map: TimeHashMapConcurrent[Long, Any] = _

  before {
    map = new TimeHashMapConcurrent[Long, Any]
  }

  test("testTimelyRemove") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    assert(map.size == 1)
    Thread.sleep(1500)
    assert(map.size == 0)
  }

  test("testRemove") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.remove(1)
    assert(map.size == 0)
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.put(2, 1, FiniteDuration(1, TimeUnit.SECONDS))
    map.remove(1)
    assert(map.size == 1)
  }

  test("testSize") {
    assert(map.size == 0)
    map.put(1, 1, FiniteDuration(500, TimeUnit.SECONDS))
    assert(map.size == 1)
  }

  test("testIRI") {
    map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))
    assert(map.size == 1)
    map.remove(1)
    assert(map.size == 0)
    map.put(1, 1, FiniteDuration(3, TimeUnit.SECONDS))
    assert(map.size == 1)
    Thread.sleep(1500)
    assert(map.size == 1)
    Thread.sleep(2000)
    assert(map.size == 0)
  }

  test("should return last value") {
    map.put(10, 10, FiniteDuration(3, TimeUnit.SECONDS))
    map.put(10, 11, FiniteDuration(5, TimeUnit.SECONDS))
    assert(map.get(10) == Some(11))

    assert(map.size == 1)
  }

  test("parallel put") {

    threadNamed("t1") {
      for (i <- 1 to 10) yield {
        map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
      }
    }

    threadNamed("t2") {
      for (i <- 11 to 20) yield {
        map.put(i, Some(i), FiniteDuration(2, TimeUnit.SECONDS))
      }
    }

    whenFinished {
      assert(map.size == 20)
      Thread.sleep(2000 * 10 + 1000 * 10)
      assert(map.size == 0)
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
      assert(map.size == 10)
      Thread.sleep(2000 * 10)
      assert(map.size == 0)
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
        assert(map.remove(i) == Some(i))
      }
    }

    whenFinished {
      assert(map.size == 0)
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
      assert(map.size == 0)
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


    assert(map.size == 0)
  }

  test("parallel single put and remove- remove can happen before put") {
    threadNamed("t1") {
      map.put(10, 10, FiniteDuration(3, TimeUnit.SECONDS))
    }

    threadNamed("t2") {
      for (i <- 1 to 3) yield {
        map.remove(i)
      }
    }

    assert(map.size == 0)
  }


}
