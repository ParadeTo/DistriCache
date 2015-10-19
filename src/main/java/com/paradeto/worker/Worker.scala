package com.paradeto.worker

import java.text.SimpleDateFormat
import java.util.Date
import com.paradeto.entity.Star
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import akka.actor.{Props, ActorSystem, ActorSelection, Actor}
import akka.actor.Actor.Receive
import com.paradeto.message._
import net.sf.ehcache.{CacheManager, Cache}
import scala.concurrent.duration._

/**
 * Created by Ayou on 2015/10/15.
 */
class Worker(host: String, port: Integer, master: ActorSelection) extends Actor {
  private val ehcacheService: EhcacheService = new EhcacheService
  private val HEARTBEAT_MILLIS = 4 * 1000
  private val workerId = generateWorkerId()

  override def receive: Receive = {

    case RegisteredWorker =>
      println("收到master的注册成功信息")
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)

    case SendHeartbeat =>
      println("发送心跳")
      val keys = ehcacheService.getKeys
      val heartbeat = new Heartbeat(workerId, keys.toList)
      master ! heartbeat

    case Register =>
      val register = new Register(workerId, host, port)
      master ! register

    //来自客户端，保存数据的消息,直接发给老大解决
    case SaveData(data, num) =>
      master ! SaveData(data, num)

    // worker判断数据是否在本地，没有
    // 就发给老大,这里用forward，则
    // master收到信息的sender是client，master直接把数据存放的位置发给client
    case ReadData(key) =>
      if(ehcacheService.exist(key)){
        println("key:"+key)
        val star:Star = new Star(ehcacheService.getOne(key).id,ehcacheService.getOne(key).name,
          ehcacheService.getOne(key).age,ehcacheService.getOne(key).vocation)
        sender ! star
      }else{
        master.forward(ReadData(key))
      }


    //保存数据
    case data: (Int, Star) =>
      println("保存数据")
      ehcacheService.saveOrUpdate(data._1, data._2)
  }


  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }


}

object Worker {
  def main(args: Array[String]) {
    val system = ActorSystem("Worker", ConfigFactory.load().getConfig("Worker"))
    val host = ConfigFactory.load().getString("Worker.akka.remote.netty.tcp.hostname")
    val port = ConfigFactory.load().getInt("Worker.akka.remote.netty.tcp.port")
    val master = system.actorSelection("akka.tcp://Master@127.0.0.1:5101/user/master")
    val worker = system.actorOf(Props(classOf[Worker], host, port, master), name = "worker")
    worker ! Register
  }
}
