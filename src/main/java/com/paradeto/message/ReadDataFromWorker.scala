package com.paradeto.message

import akka.actor.ActorRef

/**
 * Created by Ayou on 2015/10/17.
 */
case class ReadDataFromWorker (worker:ActorRef,key:Int) extends Serializable
