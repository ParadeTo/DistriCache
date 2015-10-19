package com.paradeto.master

import akka.actor.ActorRef

/**
 * Created by Ayou on 2015/10/15.
 */
class WorkerInfo(
  val id:String,
  val host:String,
  val port:Int,
  val actorRef:ActorRef)
  extends Serializable{
  private val serialVersionUID = - 2559771493190544792L
  @transient var lastHeartbeat: Long = _
  @transient var isLoad:Boolean = false
}
