package map.TimedConcurrentHashMap

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorSystem, Scheduler}
import map.ActorsTimedConcurrentHashMap.StampedObject

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

trait Size {
  def size: Long
}

trait TimeBasedHashMapConcurrent[K, V] {
  def put(k: K, v: V, duration: FiniteDuration)

  def get(k: K): Option[V]

  def remove(k: K): Option[V]
}

class TimeHashMapConcurrent[K, V] extends TimeBasedHashMapConcurrent[K, V] with Size {
  val actorSystem: ActorSystem = ActorSystem("TimeHashMapConcurrent")
  val scheduler: Scheduler = actorSystem.scheduler
  implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher
  var counter = new AtomicInteger(0)
  val map: concurrent.Map[K, StampedObject[V]] = new ConcurrentHashMap[K, StampedObject[V]]().asScala

  override def put(k: K, v: V, duration: FiniteDuration): Unit = synchronized {
    val stamp = counter.getAndIncrement
    map.put(k, StampedObject(v, stamp))
    scheduler.scheduleOnce(duration, () => tryRemove(k, stamp))
  }

  override def get(k: K): Option[V] = synchronized {
    map.get(k).map(stampedObject => stampedObject.value)
  }

  override def remove(k: K): Option[V] = synchronized {
    map.remove(k).map(stampedObject => stampedObject.value)
  }

  private def tryRemove(k: K, stamp: Long): Unit = synchronized {
    map.get(k)
      .filter(stampedObject => stampedObject.stamp == stamp)
      .foreach(_ => map.remove(k))
  }

  override def size: Long = map.size

  override def toString = s"TimeHashMapConcurrent($map, $size)"
}
