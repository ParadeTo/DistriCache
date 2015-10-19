package com.paradeto

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor, Props, ActorSystem}
import com.paradeto.entity.Star
import com.paradeto.message.{ReadDataFromWorker, ReadData, SaveData}
import com.typesafe.config.ConfigFactory

/**
 * Created by Ayou on 2015/10/17.
 */
class Client extends Actor {
  override def receive: Receive = {
    case msg:String =>
      println(msg)
    case ReadDataFromWorker (worker:ActorRef,key:Int)=>
      worker ! ReadData(key)

    case star:Star=>
      println(star)


  }
}
object Client{
  def main(args: Array[String]) {
    val system = ActorSystem("Client", ConfigFactory.load().getConfig("Client"))
    val master = system.actorSelection("akka.tcp://Master@127.0.0.1:5101/user/master")
    val worker = system.actorSelection("akka.tcp://Worker@127.0.0.1:5102/user/worker")
    val client = system.actorOf(Props(classOf[Client]), name = "client")

    // 保存
    for(x <- 1 to 20){
      var star = new Star(x,"Jordan"+x,55,"basketball player")
      worker ! SaveData((star.id,star),2)
      Thread.sleep(2000)
    }

    //读取
    for(x <- 1 to 20)
      worker ! ReadData(x)
    Thread.sleep(1000)
  }


}
