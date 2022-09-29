package com.wiley.epdcs.eventhub.core

import com.typesafe.config.ConfigFactory

trait ConfigProvider {

  def env = {
    val c = ConfigFactory.load()
    var env = System.getProperty("env")
    if (env == null) {
      env = c.getString("env")
    }
    env
  }

  def config = {
    val c = ConfigFactory.load()
    val envStr = env

    c.getConfig(s"environments.$envStr").withFallback(c)
  }

  def executionConfigs = {
    val c = ConfigFactory.load()
    var cleanUp = System.getProperty("cleanUpDataAfterTest")
    if (cleanUp == null) {
      cleanUp = c.getString("cleanUpDataAfterTest")
    }
    cleanUp
  }

}
