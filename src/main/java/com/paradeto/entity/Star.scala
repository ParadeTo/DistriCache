package com.paradeto.entity

/**
 * Created by Ayou on 2015/10/16.
 */
class Star(val id:Int,val name:String,val age:Int,val vocation:String) extends Serializable{
  override def toString: String ={
    "id:"+id+",name:"+name+",age:"+age+",vocation:"+vocation;
  }
}

