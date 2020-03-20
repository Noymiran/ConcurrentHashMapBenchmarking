package benchmarking

import java.util.concurrent.TimeUnit

import map.TimedConcurrentHashMap.TimeHashMapConcurrent
import org.openjdk.jmh.annotations._

import scala.concurrent.duration.FiniteDuration

@Threads(10)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput, Mode.SampleTime))
@State(Scope.Benchmark)
class TimeHashMapConcurrentBM {

  var map: TimeHashMapConcurrent[Long, Any] = _

  @Setup(Level.Trial)
  def prepare = {
    map = new TimeHashMapConcurrent[Long, Any]

    for (i <- 1 to 10) yield {
      map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }

    println("done prepare")
  }

  @Benchmark
  def putKey(state: TimeHashMapConcurrentBM) =
    state.map.put(0, Some(0), FiniteDuration(1, TimeUnit.SECONDS))

  @Benchmark
  def getSize(state: TimeHashMapConcurrentBM) =
    state.map.size

  @Benchmark
  def getValue(state: TimeHashMapConcurrentBM) =
    state.map.get(0)

  @Benchmark
  def removeKey(state: TimeHashMapConcurrentBM) =
    state.map.remove(0)

  @Benchmark
  def putKeys(state: TimeHashMapConcurrentBM) = {
    for (i <- 1 to 10) yield {
      state.map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Benchmark
  def getValues(state: TimeHashMapConcurrentBM) = {
    for (i <- 1 to 10) yield {
      state.map.get(i)
    }
  }

  @Benchmark
  def removeKeys(state: TimeHashMapConcurrentBM) = {
    for (i <- 1 to 10) yield {
      state.map.remove(i)
    }
  }


  @Group("removeAndPut")
  @GroupThreads(1)
  @Benchmark
  def putRemoveKeyParallel(state: TimeHashMapConcurrentBM) =
    state.map.put(0, 0, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("removeAndPut")
  @GroupThreads(1)
  @Benchmark
  def removeKeyParallel(state: TimeHashMapConcurrentBM) =
    state.map.remove(0)


  @Group("sizeAndPut")
  @GroupThreads(1)
  @Benchmark
  def putKeyAndSizeParallel(state: TimeHashMapConcurrentBM) =
    state.map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("sizeAndPut")
  @GroupThreads(1)
  @Benchmark
  def getSizeParallel(state: TimeHashMapConcurrentBM) =
    state.map.size

  @Group("getValueAndPut")
  @GroupThreads(1)
  @Benchmark
  def putKeyAndGetParallel(state: TimeHashMapConcurrentBM) =
    state.map.put(2, 2, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("getValueAndPut")
  @GroupThreads(1)
  @Benchmark
  def getValueParallel(state: TimeHashMapConcurrentBM) =
    state.map.get(2)


  @Group("removeAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def putKeysParallel(state: TimeHashMapConcurrentBM) = {
    for (i <- 10 to 20) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("removeAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def removeKeysParallel(state: TimeHashMapConcurrentBM) = {
    for (i <- 10 to 20) yield {
      state.map.remove(i)
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def putKeysAndGetValuesParallel(state: TimeHashMapConcurrentBM) = {
    for (i <- 30 to 40) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(2)
  @Benchmark
  def getValuesParallel(state: TimeHashMapConcurrentBM) = {
    for (i <- 30 to 40) yield {
      state.map.get(i)
    }
  }
}
