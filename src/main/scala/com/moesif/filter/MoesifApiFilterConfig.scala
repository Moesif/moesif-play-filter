package com.moesif.filter

import play.api.Configuration

import javax.inject.{Inject, Provider, Singleton}

/**
  * Configuration for the moesif api filter
  * @param maxApiEventsToHoldInMemory number of API events that can be cached in memory before sending to Moesif
  * @param moesifApplicationId Your Moesif Application Id can be found in the Moesif Portal. After signing up for a Moesif account, your Moesif Application Id
  *                            will be displayed during the onboarding steps. You can always find your Moesif Application Id at any time by logging
  *                            into the Moesif Portal, click on the top right menu, and then clicking Installation.
  * @param maxBatchTime Int of the max time in milliseconds to buffer events before sending
  */
case class MoesifApiFilterConfig(maxApiEventsToHoldInMemory: Int, batchSize: Int, moesifApplicationId: String, requestBodyProcessingEnabled: Boolean = true,
                                 moesifCollectorEndpoint: String, maxBatchTime: Int = 2000, useGzip: Boolean = false, debug: Boolean = false, responseBodyProcessingEnabled: Boolean = true, reqBodySizeLimit: Int = 100000, resBodySizeLimit: Int = 100000, maxBatchSize: Int = 9000000)
object MoesifApiFilterConfig {
  def fromConfiguration(conf: Configuration): MoesifApiFilterConfig = {
    val config = conf.get[Configuration]("play.filters.moesifApiFilter")
    val maxApiEventsToHoldInMemory = config.getOptional[Int]("maxApiEventsToHoldInMemory").getOrElse(100000)
    val batchSize = config.getOptional[Int]("batchSize").getOrElse(200)
    val reqBodyProcessing = config.getOptional[Boolean]("requestBodyProcessingEnabled").getOrElse(false)
    val resBodyProcessing = config.getOptional[Boolean]("responseBodyProcessingEnabled").getOrElse(true)
    val reqBodySizeLimit = config.getOptional[Int]("requestBodySizeLimit").getOrElse(100000)
    val resBodySizeLimit = config.getOptional[Int]("responseBodySizeLimit").getOrElse(100000)
    val debug = config.getOptional[Boolean]("debug").getOrElse(false)
    val maxBatchTime = config.getOptional[Int]("maxBatchTime").getOrElse(2000)
    val moesifApplicationId = config.get[String]("moesifApplicationId")
    val moesifCollectorEndpoint = config.get[String]("collectorEndpoint")
    val useGzip = config.getOptional[Boolean]("useGzip").getOrElse(false)
    val maxBatchSize = config.getOptional[Int]("maxBatchSize").getOrElse(100000000)
    MoesifApiFilterConfig(maxApiEventsToHoldInMemory, batchSize, moesifApplicationId, reqBodyProcessing, moesifCollectorEndpoint, maxBatchTime, useGzip, debug, resBodyProcessing, reqBodySizeLimit, resBodySizeLimit, maxBatchSize)
  }
}

/**
  * The gzip filter configuration provider.
  */
@Singleton
class MoesifApiFilterConfigProvider @Inject()(config: Configuration) extends Provider[MoesifApiFilterConfig] {
  lazy val get = MoesifApiFilterConfig.fromConfiguration(config)
}

