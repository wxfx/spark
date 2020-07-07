package Charpter5

import com.typesafe.config.{Config, ConfigFactory}

object ParamsApp extends App{
  private val config: Config = ConfigFactory.load()
  private val url: String = config.getString("db.default.url")
  println(url)
}
