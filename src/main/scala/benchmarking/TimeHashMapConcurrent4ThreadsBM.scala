package benchmarking

import java.util.concurrent.TimeUnit

import map.ActorsTimedConcurrentHashMap.ActorSystemRef
import map.TimedConcurrentHashMap.TimeHashMapConcurrentExternalActor
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
class TimeHashMapConcurrent4ThreadsBM {

  var map: TimeHashMapConcurrentExternalActor[Long, Any] = _
  val duration: Duration = 15.seconds


  @Setup(Level.Trial)
  def prepare = {
    map = new TimeHashMapConcurrentExternalActor[Long, Any](ActorSystemRef.system)

    for (i <- 1 to 10) yield {
      map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }

    println("done prepare")
  }

  @Benchmark
  def putKey(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.put(0, Some(0), FiniteDuration(1, TimeUnit.SECONDS))

  @Benchmark
  def getSize(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.size

  @Benchmark
  def getValue(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.get(0)

  @Benchmark
  def removeKey(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.remove(0)

  @Benchmark
  def putKeys(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 1 to 10) yield {
      state.map.put(i, Some(i), FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Benchmark
  def getValues(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 1 to 10) yield {
      state.map.get(i)
    }
  }

  @Benchmark
  def removeKeys(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 1 to 10) yield {
      state.map.remove(i)
    }
  }


  @Group("removeAndPut")
  @GroupThreads(4)
  @Benchmark
  def putRemoveKeyParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.put(0, 0, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("removeAndPut")
  @GroupThreads(4)
  @Benchmark
  def removeKeyParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.remove(0)


  @Group("sizeAndPut")
  @GroupThreads(4)
  @Benchmark
  def putKeyAndSizeParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.put(1, 1, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("sizeAndPut")
  @GroupThreads(4)
  @Benchmark
  def getSizeParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.size

  @Group("getValueAndPut")
  @GroupThreads(4)
  @Benchmark
  def putKeyAndGetParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.put(2, 2, FiniteDuration(1, TimeUnit.SECONDS))

  @Group("getValueAndPut")
  @GroupThreads(4)
  @Benchmark
  def getValueParallel(state: TimeHashMapConcurrent4ThreadsBM) =
    state.map.get(2)


  @Group("removeAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def putKeysParallel(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 10 to 20) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("removeAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def removeKeysParallel(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 10 to 20) yield {
      state.map.remove(i)
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def putKeysAndGetValuesParallel(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 30 to 40) yield {
      state.map.put(i, i, FiniteDuration(1, TimeUnit.SECONDS))
    }
  }

  @Group("getAndPutMultiple")
  @GroupThreads(4)
  @Benchmark
  def getValuesParallel(state: TimeHashMapConcurrent4ThreadsBM) = {
    for (i <- 30 to 40) yield {
      state.map.get(i)
    }
  }

  @TearDown(Level.Trial)
  def shutdown(): Unit = {
    Await.ready(ActorSystemRef.system.terminate(), duration)
  }
}
