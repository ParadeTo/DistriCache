package com.paradeto.message

/**
 * 心跳包括workerId和缓存的keys列表
 * Created by Ayou on 2015/10/15.
 */
case class Heartbeat(workerId: String,keys:List[_]) extends Serializable
