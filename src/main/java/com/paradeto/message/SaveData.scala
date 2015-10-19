package com.paradeto.message

import com.paradeto.entity.Star

/**
 * 数据及备份的份数
 * Created by Ayou on 2015/10/16.
 */
case class SaveData(data:(Int,Star),num:Int) extends Serializable
