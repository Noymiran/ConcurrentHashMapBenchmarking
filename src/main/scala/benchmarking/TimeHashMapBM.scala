package benchmarking

import java.util.concurrent.TimeUnit

import map.ActorsTimedConcurrentHashMap.{ActorSystemRef, TimeHashMap}
import org.openjdk.jmh.annotations._

import scala.concurrent.duration._

@Threads(10)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput, Mode.SampleTime))
@State(Scope.Benchmark)
class TimeHashMapBM {
  var map: TimeHashMap[Long, Any] = _

  @Setup(Level.Trial)
  def prepare = {
    map = new TimeHashMap[Long, Any](ActorSystemRef.system)

    for (i <- 1 to 10) yield {
      map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }
    println("done prepare")
  }

  @Benchmark
  def putKey(state: TimeHashMapBM) =
    state.map.put(0, Some(0), FiniteDuration(1, TimeUnit.SECONDS))

  @Benchmark
  def getSize(state: TimeHashMapBM) = {
    state.map.size
  }

  @Benchmark
  def getValue(state: TimeHashMapBM) =
    state.map.get(0)

  @Benchmark
  def removeKey(state: TimeHashMapBM) =
    state.map.remove(0)

  @Benchmark
  def putKeys(state: TimeHashMapBM) = {
    for (i <- 1 to 10) yield {
      state.map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Benchmark
  def getValues(state: TimeHashMapBM) = {
    for (i <- 1 to 10) yield {
      state.map.get(i)
    }
  }

  @Benchmark
  def removeKeys(state: TimeHashMapBM) = {
    for (i <- 1 to 10) yield {
      state.map.remove(i)
    }
  }


  @Group("removeAndPut")
  @GroupThreads(1)
  @Benchmark
  def putRemoveKeyParallel(state: TimeHashMapBM) =
    state.map.put(0, 0, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("removeAndPut")
  @GroupThreads(1)
  @Benchmark
  def removeKeyParallel(state: TimeHashMapBM) =
    state.map.remove(0)


  @Group("sizeAndPut")
  @GroupThreads(1)
  @Benchmark
  def putKeyAndSizeParallel(state: TimeHashMapBM) =
    state.map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("sizeAndPut")
  @GroupThreads(1)
  @Benchmark
  def getSizeParallel(state: TimeHashMapBM) =
    state.map.size

  @Group("getValueAndPut")
  @GroupThreads(1)
  @Benchmark
  def putKeyAndGetParallel(state: TimeHashMapBM) =
    state.map.put(2, 2, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("getValueAndPut")
  @GroupThreads(1)
  @Benchmark
  def getValueParallel(state: TimeHashMapBM) =
    state.map.get(2)


  @Group("removeAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def putKeysParallel(state: TimeHashMapBM) = {
    for (i <- 10 to 20) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("removeAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def removeKeysParallel(state: TimeHashMapBM) = {
    for (i <- 10 to 20) yield {
      state.map.remove(i)
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def putKeysAndGetValuesParallel(state: TimeHashMapBM) = {
    for (i <- 30 to 40) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def getValuesParallel(state: TimeHashMapBM) = {
    for (i <- 30 to 40) yield {
      state.map.get(i)
    }
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    ActorSystemRef.system.terminate()
  }
}