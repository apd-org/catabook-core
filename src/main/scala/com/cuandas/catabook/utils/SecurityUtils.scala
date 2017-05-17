package com.cuandas.catabook.utils

/**
  * Created by chenxm on 17-4-4.
  */
object SecurityUtils {

  def md5(s: String, len: Int = 16) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(len)
  }
}
