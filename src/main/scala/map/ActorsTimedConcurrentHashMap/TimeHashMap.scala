package map.ActorsTimedConcurrentHashMap

import akka.actor.{Actor, ActorSystem, Props, Timers}
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._

trait TimeBasedHashMap[K, V] {
  def put(k: K, v: V, duration: FiniteDuration): Future[Unit]

  def get(k: K): Future[Option[V]]

  def remove(k: K): Future[Option[V]]

  def size(): Future[Int]
}

object ActorSystemRef {

  import com.typesafe.config.ConfigFactory

  val config = ConfigFactory.load("application.conf")
  val system = ActorSystem("TimeHashMapActorSystemRef", config.getConfig("PerformanceApp").withFallback(config))
}

class TimeHashMap[K, V](system: ActorSystem) extends TimeBasedHashMap[K, V] {

  import TimedActor._

  implicit val timeout = Timeout(3.seconds)
  val actor = system.actorOf(Props[TimedActor[K, V]].withDispatcher("akka.actor.my-dispatcher"))

  override def put(k: K, v: V, duration: FiniteDuration): Future[Unit] = actor.ask(Insert(k, v, duration)).mapTo[Unit]


  override def get(k: K): Future[Option[V]] = actor.ask(Get(k)).mapTo[Option[V]]


  override def remove(k: K): Future[Option[V]] = actor.ask(Remove(k)).mapTo[Option[V]]


  override def size(): Future[Int] = actor.ask(Size).mapTo[Int]

}

class TimedActor[K, V] extends Actor with Timers {

  import TimedActor._

  val map = collection.mutable.Map[K, StampedObject[V]]()
  var counter: Long = 0
  implicit val executionContext = context.system.dispatchers.lookup("akka.actor.my-dispatcher")

  override def receive: Receive = {
    case Insert(k: K, v: V, duration: FiniteDuration) =>
      val stampedObject = StampedObject(v, counter)
      map.update(k, stampedObject)
      counter += 1
      val expectedCounter = counter - 1
      context.system.scheduler.scheduleOnce(duration) {
        self.tell(TryRemove(k, expectedCounter), self)
      }
    case Size =>
      sender() ! map.size

    case Remove(k: K) =>
      val result = map.remove(k)
      sender() ! result.map(_.value)

    case TryRemove(k: K, stamp: Long) =>
      map.get(k)
        .filter { stamped =>
          stamped.stamp == stamp
        }
        .foreach(_ => map.remove(k))

    case Get(k: K) =>
      sender() ! map.get(k).map(_.value)
  }

  override def toString = s"TimedActor($map)"
}

case class StampedObject[K](value: K, stamp: Long)

object TimedActor {

  case class Insert[K, V](k: K, v: V, duration: Duration)

  case class Remove[K](k: K)

  case class Get[K](k: K)

  case class TryRemove[K](k: K, stamp: Long)

  case object Size

}

