package map.TimedConcurrentHashMap

import java.util.concurrent.atomic.AtomicInteger
import map.ActorsTimedConcurrentHashMap.StampedObject

import scala.collection.convert.decorateAsScala._
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorSystem, Scheduler}

import scala.collection.concurrent
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._



class TimeHashMapConcurrentExternalActor[K, V](system:ActorSystem) extends TimeBasedHashMapConcurrent[K, V] with Size {
  val scheduler: Scheduler = system.scheduler
  implicit val executor: ExecutionContextExecutor = system.dispatcher
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
