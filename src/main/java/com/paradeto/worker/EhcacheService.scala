package com.paradeto.worker

import com.paradeto.entity.Star
import net.sf.ehcache.{Element, CacheManager, Cache}

/**
 * Ehcache操作类
 * Created by Ayou on 2015/10/17.
 */
class EhcacheService {
  val starCache: Cache = CacheManager.create("src/main/resources/ehcache.xml").getCache("starCache")
  val limit: Integer = 3

  def saveOrUpdate(key:Int,star: Star) {
    val elem: Element = new Element(key, star)
    starCache.put(elem)
  }

  def getOne(id: Int): Star = {
    val elem: Element = starCache.get(id)
    val s: Star = elem.getValue.asInstanceOf[Star]
    return s
  }

  def getSize: Integer = starCache.getSize

  def getKeys = starCache.getKeys

  def exist(key:Int) = if(starCache.getKeys == null || !starCache.getKeys.contains(key)) false else true
}
