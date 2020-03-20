package benchmarking

import java.util.concurrent.TimeUnit

import map.ActorsTimedConcurrentHashMap.{ActorSystemRef, TimeHashMap}
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration._

@Threads(10)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput, Mode.SampleTime))
@State(Scope.Benchmark)
class TimeHashMapFutureBM {
  var map: TimeHashMap[Long, Any] = _
  val duration: Duration = 15.seconds

  @Setup(Level.Trial)
  def prepare = {
    map = new TimeHashMap[Long, Any](ActorSystemRef.system)

    for (i <- 1 to 10) yield {
      map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }
    println("done prepare")
  }

  @Benchmark
  def putKey(state: TimeHashMapFutureBM) =
    Await.ready(state.map.put(0, Some(0), FiniteDuration(1, TimeUnit.SECONDS)), duration)


  @Benchmark
  def getSize(state: TimeHashMapFutureBM) = {
    Await.ready(state.map.size, duration)
  }

  @Benchmark
  def getValue(state: TimeHashMapFutureBM) =
    Await.ready(state.map.get(0), duration)

  @Benchmark
  def removeKey(state: TimeHashMapFutureBM) =
    Await.ready(state.map.remove(0), duration)

  @Benchmark
  def putKeys(state: TimeHashMapFutureBM) = {
    for (i <- 1 to 10) yield {
      Await.ready(state.map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS)), duration)
    }
  }

  @Benchmark
  def getValues(state: TimeHashMapFutureBM) = {
    for (i <- 1 to 10) yield {
      Await.ready(state.map.get(i), duration)
    }
  }

  @Benchmark
  def removeKeys(state: TimeHashMapFutureBM) = {
    for (i <- 1 to 10) yield {
      Await.ready(state.map.remove(i), duration)
    }
  }


  @Group("removeAndPut")
  @GroupThreads(4)
  @Benchmark
  def putRemoveKeyParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.put(0, 0, FiniteDuration(1, TimeUnit.SECONDS)), duration)

  @Group("removeAndPut")
  @GroupThreads(4)
  @Benchmark
  def removeKeyParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.remove(0), duration)


  @Group("sizeAndPut")
  @GroupThreads(4)
  @Benchmark
  def putKeyAndSizeParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS)), duration)

  @Group("sizeAndPut")
  @GroupThreads(4)
  @Benchmark
  def getSizeParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.size, duration)

  @Group("getValueAndPut")
  @GroupThreads(4)
  @Benchmark
  def putKeyAndGetParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.put(2, 2, FiniteDuration(1, TimeUnit.SECONDS)), duration)

  @Group("getValueAndPut")
  @GroupThreads(4)
  @Benchmark
  def getValueParallel(state: TimeHashMapFutureBM) =
    Await.ready(state.map.get(2), duration)


  @Group("removeAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def putKeysParallel(state: TimeHashMapFutureBM) = {
    for (i <- 10 to 20) yield {
      Await.ready(state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS)), duration)
    }
  }

  @Group("removeAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def removeKeysParallel(state: TimeHashMapFutureBM) = {
    for (i <- 10 to 20) yield {
      Await.ready(state.map.remove(i), duration)
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def putKeysAndGetValuesParallel(state: TimeHashMapFutureBM) = {
    for (i <- 30 to 40) yield {
      Await.ready(state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS)), duration)
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def getValuesParallel(state: TimeHashMapFutureBM) = {
    for (i <- 30 to 40) yield {
      Await.ready(state.map.get(i), duration)
    }
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    Await.ready(ActorSystemRef.system.terminate(), duration)
  }
}