package com.paradeto.master


import com.paradeto.message._
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable._
import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive

import scala.concurrent.duration._

/**
 * Created by Ayou on 2015/10/15.
 */
class Master extends Actor {
  // 存储worker信息的set
  val workers = new HashSet[WorkerInfo]
  // 通过id查worker
  val id2worker: Map[String, WorkerInfo] = new HashMap[String, WorkerInfo];
  // 存储缓存信息的map:key 缓存的key value workerInfoMap的key
  val cacheInfoMap: Map[Int, List[String]] = new HashMap[Int, List[String]]
  // worker过期时间,毫秒
  val WORKER_TIMEOUT = 5 * 1000

  override def receive: Receive = {
    case Register(id, host, port) =>
      println("收到注册信息:" + id)
      val work = new WorkerInfo(id, host, port, sender)
      workers += work
      id2worker.put(id, work)
      //DEBUG
      println("注册后的workers")
      println(workers)
      println("注册后的id2workers")
      println(id2worker)
      sender ! RegisteredWorker

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case Heartbeat(workerId, keys) =>
      println("收到心跳信息:" + workerId + "-keys_size:" + keys.size)
      id2worker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
          if (!keys.isEmpty) {
            for (k <- keys) //更新缓存映射
              cacheInfoMap.filter(_._1 != k).map(x => x._2.toBuffer -= workerId)
          } else {
            //如果worker中没有缓存了
            for (m <- cacheInfoMap)
              m._2.toBuffer -= workerId
          }
        case None =>
      }


    case SaveData(data, num) =>
      println("来自客户端保存文件的消息")
      //DEBUG
      val workIdList = loadBalance(num)
      if (!workIdList.isEmpty)
        save(data, workIdList)

    case ReadData(id) =>
      println("key:" + id)
      read(id)

    case _ =>
      sender
      println("test")
  }


  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)
  }

  private def timeOutDeadWorkers(): Unit = {
    println("检查失效worker-" + System.currentTimeMillis())
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      println("失效worker为" + worker.id)
      println("Removing %s because we got no heartbeat in %d seconds".format(
        worker.id, WORKER_TIMEOUT / 1000))
      //源码有尝试重连，我就不考虑了，再写就没完没了了
      id2worker -= worker.id
      workers -= worker
      for (m <- cacheInfoMap) {
        val workerIdList = m._2.toBuffer
        workerIdList -= worker.id
        cacheInfoMap.put(m._1, workerIdList.toList)
      }


      println("worker失效后的cacheInfoMap")
      println(cacheInfoMap)
      println("worker失效后的workers")
      println(workers)
      println("worker失效后的id2worker")
      println(id2worker)
    }
  }

  /**
   * 返回数据存储的workerId列表
   * @param num
   * @return
   */
  private def loadBalance(num: Int): List[String] = {
    val workerIdList = new ListBuffer[String]
    // 数据份数不能超过worker的数量
    val n = if (num > id2worker.size) id2worker.size else num
    var i = 0

    // 如果cacheInfoMap为空,随便存
    if (cacheInfoMap.isEmpty) {
      for ((workId, worker) <- id2worker if i < n) {
        workerIdList += workId
        worker.isLoad = true
        i += 1
      }
    } else {
      // 先从workers中负载为0的开始选
      println("workders的负载信息")
      for (worker <- workers) {
        println(worker.id + "-" + worker.isLoad)
        if (!worker.isLoad && i < n) {
          workerIdList += worker.id
          worker.isLoad = true
          i += 1
        }
      }
      if (i < n) {
        //还不够就从负载不为0的worker中选

        // 得到每个work的负载(workId,num)
        val loadOfWork = new mutable.HashMap[String, Int]
        cacheInfoMap.map(item =>
          item._2.map(x => (x, item._1))
        ).flatMap(x => x).foreach(x =>
          if (loadOfWork.contains(x._1)) {
            val v = loadOfWork.get(x._1).get + 1
            loadOfWork.put(x._1, v)
          } else {
            loadOfWork.put(x._1, 0)
          })
        // 按负载排序
        val sortLoad = loadOfWork.toList.sortWith((x, y) => x._2 < y._2)

        println("sortLoad:")
        println(sortLoad)
        // 取负载低的前n个
        for ((workId, load) <- sortLoad if i < n) {
          workerIdList += workId
          i += 1
        }
      }
    }
    println(workerIdList)
    return workerIdList.toList
  }

  /**
   * 保存数据
   * @param data
   * @param workerIdList
   */
  private def save(data: (Int, _), workerIdList: List[String]): Unit = {
    cacheInfoMap.put(data._1.toInt, workerIdList)
    for (workerId <- workerIdList) {
      val v = cacheInfoMap.get(data._1).get
      if (!v.contains(workerId))
        v.toBuffer += workerId
      val worker = id2worker.get(workerId).getOrElse(None)
      // 如果该actor不存在
      if (worker != None)
        worker.asInstanceOf[WorkerInfo].actorRef ! data
    }
  }

  private def read(id: Int): Unit = {
    val workerIdList = cacheInfoMap.get(id).getOrElse(None)
    println("workerIdList:" + workerIdList)
    if (workerIdList == null || workerIdList == None || workerIdList.asInstanceOf[List[String]].isEmpty) {
      sender ! "数据不存在"
    } else {
      var flag = true
      for (workerId <- workerIdList.asInstanceOf[List[String]] if flag) {
        val worker = id2worker.get(workerId).getOrElse(None)
        println("worker:" + worker)
        if (worker != None) {
          // 这里的sender是client
          sender ! ReadDataFromWorker(worker.asInstanceOf[WorkerInfo].actorRef, id)
          flag = false
        }
      }
    }
  }

  private def DEBUG(): Unit = {
    println("workers:")
    println(workers)
    println("id2worker:")
    println(id2worker)
    println("cache")
    println(cacheInfoMap)
  }
}

object Master {
  def main(args: Array[String]) {
    val system = ActorSystem("Master", ConfigFactory.load().getConfig("Master"))
    val sleepActor = system.actorOf(Props[Master], name = "master")
  }
}