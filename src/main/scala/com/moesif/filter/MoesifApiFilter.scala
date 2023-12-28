package com.moesif.filter

import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Materializer}
import akka.util.ByteString
import com.moesif.api.http.client.{APICallBack, HttpContext}
import com.moesif.api.http.response.HttpResponse
import com.moesif.api.models._
import com.moesif.api.{Base64, MoesifAPIClient, BodyParser => MoesifBodyParser}
import play.api.Configuration
import play.api.http.HttpEntity
import play.api.inject.{SimpleModule, bind}
import play.api.libs.streams.Accumulator
import play.api.mvc.{EssentialAction, EssentialFilter, RequestHeader, Result}

import java.util.Date
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.logging._
import javax.inject.{Inject, Singleton}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}
/**
  * MoesifApiFilter
  * logs API calls and sends to Moesif for API analytics and log analysis.
  */
@Singleton
class MoesifApiFilter @Inject()(config: MoesifApiFilterConfig)(implicit mat: Materializer) extends  EssentialFilter  {
  private val requestBodyParsingEnabled = config.requestBodyProcessingEnabled
  private val maxApiEventsToHoldInMemory = config.maxApiEventsToHoldInMemory
  private val batchSize = config.batchSize
  private val maxBatchTime = config.maxBatchTime
  private var lastSendTime = System.currentTimeMillis()
  private val moesifApplicationId = config.moesifApplicationId
  private val debug = config.debug
  private val moesifCollectorEndpoint = config.moesifCollectorEndpoint
  private val eventModelBuffer = mutable.ArrayBuffer[EventModel]()
  private val client = new MoesifAPIClient(moesifApplicationId, moesifCollectorEndpoint)
  private val moesifApi = client.getAPI
  private val useGzip = config.useGzip

  val eventBufferFlusher: Runnable = new Runnable() {
    override def run(): Unit = {
      if(debug){
        logger.log(Level.INFO, "flush events by scheduler...")
      }
      flushEventBuffer()
    }
  }
  // Create an executor to fetch application config every 5 minutes
  val exec: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  // Initialize with a ScheduledFuture[_] that fires immediately, doing nothing
  private var scheduledSend: ScheduledFuture[_] = exec.schedule(eventBufferFlusher, 0, TimeUnit.MILLISECONDS)

  private val logger = Logger.getLogger("moesif.play.filter.MoesifApiFilter")
  logger.info(s"config  is $config")


  def apply(nextFilter: EssentialAction) = new EssentialAction {

    def apply(requestHeader: RequestHeader) = {
      implicit val ec = mat.executionContext

      val startTime = System.currentTimeMillis

      var accumulator: Accumulator[ByteString, Result] = nextFilter(requestHeader)

      // There is no straighforward way to get the request body. Reason being, filter will be called, as soon as there is
      // request header is available, but the request body could still be streaming in and not available yet.
      // so there is flow created, that basically tracks the request bytes that flow throw accumulator
      val requestBodyBuffer = if (requestBodyParsingEnabled) {
        val buffer = mutable.ArrayBuilder.make[Byte]
        val graph = new FullBodyFilterGraphStage(buffer)
        accumulator =  accumulator.through(Flow.fromGraph(graph))
        Some(buffer)
      } else {
        None
      }

      val reqHeaders = requestHeader.headers.headers.toMap.asJava

      val eventReqWithoutBody = new EventRequestBuilder().time(new Date()).
        uri(MoesifApiFilter.buildUri(requestHeader)).
        verb(requestHeader.method).
        apiVersion(requestHeader.version).
        ipAddress(requestHeader.remoteAddress).
        headers(reqHeaders)

      lazy val eventReqWithBody =  requestBodyBuffer.map(_.result()) match {
        case Some(buffer) if buffer.nonEmpty =>
          val requestBodyStr = new String(buffer)
          val reqBodyParsed = MoesifBodyParser.parseBody(reqHeaders, requestBodyStr)
          eventReqWithoutBody.body(reqBodyParsed.body).
            transferEncoding(reqBodyParsed.transferEncoding).
            build()
        case _ =>
          eventReqWithoutBody.build()
      }

      var blockedModifiedResultOpt: Option[Result] = None

      accumulator.map { result =>
        val resultHeaders = result.header.headers.asJava
        val eventRspBuilder = new EventResponseBuilder().time(new Date()).
          status(result.header.status).
          headers(resultHeaders)

        val eventModelBuilder = new EventBuilder().
          request(eventReqWithBody).
          response(eventRspBuilder.build())

        val advancedConfig = MoesifAdvancedFilterConfiguration.getConfig().getOrElse {
          MoesifAdvancedFilterConfiguration.getDefaultConfig()
        }

        if (!advancedConfig.skip(requestHeader, result)) {
          advancedConfig.sessionToken(requestHeader, result).map { sessionToken =>
            eventModelBuilder.sessionToken(sessionToken)
          }

          advancedConfig.identifyUser(requestHeader, result).map { userId =>
            eventModelBuilder.userId(userId)
          }

          advancedConfig.identifyCompany(requestHeader, result).map { companyId =>
            eventModelBuilder.companyId(companyId)
          }

          val metadata = advancedConfig.getMetadata(requestHeader, result)
          if (metadata.nonEmpty) {
            eventModelBuilder.metadata(metadata)
          }

          val eventModel: EventModel = eventModelBuilder.build()

          val blockResponse = moesifApi.getBlockedByGovernanceRulesResponse(eventModel)
          if (blockResponse.isBlocked) {
            logger.warning("Blocked by governance rules" + blockResponse.blockedBy)
            eventModel.setBlockedBy(blockResponse.blockedBy)
            eventModel.setResponse(blockResponse.response)

            blockedModifiedResultOpt = Some(result.copy(
              header = result.header.copy(
                status = eventModel.getResponse.getStatus,
                headers = eventModel.getResponse.getHeaders.asScala.toMap
              ),
              body = HttpEntity.Strict(ByteString(eventModel.getResponse.getBody.toString), result.body.contentType)
            ))
          }
          else{
            result.body.consumeData.map { resultBodyByteString =>
              val utf8String = resultBodyByteString.utf8String

              Try(MoesifBodyParser.parseBody(resultHeaders, utf8String)) match {
                case Success(bodyWrapper) if bodyWrapper.transferEncoding == "base64" =>
                  // play bytestring payload seems to be in UTF-16BE, BodyParser converts to UTF string first,
                  // which corrupts the string, use the ByteString bytes directly
                  val str = new String(Base64.encode(resultBodyByteString.toArray, Base64.DEFAULT))
                  eventModel.getResponse.setBody(str)
                  eventModel.getResponse.setTransferEncoding(bodyWrapper.transferEncoding)
                case Success(bodyWrapper) =>
                  eventModel.getResponse.setBody(bodyWrapper.body)
                  eventModel.getResponse.setTransferEncoding(bodyWrapper.transferEncoding)
                case _ => eventModel.getResponse.setBody(utf8String)
              }
            }
          }

          Try {
            sendEvent(eventModel, advancedConfig)
          } match {
            case Success(_) => Unit
            case Failure(ex) => logger.log(Level.WARNING, s"failed to send API events to Moesif: ${ex.getMessage}", ex)
          }
        }

        if (blockedModifiedResultOpt.isEmpty) {
          result
        }
        else {
          blockedModifiedResultOpt.get
        }
      }
    }
  }


  def sendEvent(eventModel: EventModel, advancedConfig: MoesifAdvancedFilterConfiguration): Unit = synchronized {
      val randomPercentage = Math.random * 100
      val sampleRateToUse = moesifApi.getSampleRateToUse(eventModel)

      val eventModelMasked = advancedConfig.maskContent(eventModel)

      // Compare percentage to send event
      if (sampleRateToUse >= randomPercentage) {
        eventModelMasked.setWeight(math.floor(100 / sampleRateToUse).toInt) // note: sampleRateToUse cannot be 0 at this point
        if(eventModelBuffer.size >= maxApiEventsToHoldInMemory){
          logger.log(Level.WARNING, s"[Moesif] Skipped Event due to event buffer size [${eventModelBuffer.size}] is over max ApiEventsToHoldInMemory ${maxApiEventsToHoldInMemory}")
        }else{
          eventModelBuffer.append(eventModelMasked)
        }
      } else {
        if(debug) {
          logger.log(Level.INFO, "[Moesif] Skipped Event due to sampleRateToUse - " + sampleRateToUse.toString + " and randomPercentage " + randomPercentage.toString)
        }
      }

      // scheduledSend below should flush the event buffer after maxBatchTime; however, we check the time here and
      // send immediately if that didn't already happen and it's time to send
      // this also has the effect of sending immediately if we are sending fewer than one event per maxBatchTime
      if (eventModelBuffer.size >= batchSize || isAfterMaxBatchTime()) {
        if(debug){
          logger.log(Level.INFO, s"[Moesif] flush events because of bucket full or time over maxBatchTime [${eventModelBuffer.size}/${maxApiEventsToHoldInMemory}] - [${System.currentTimeMillis() - lastSendTime}/${maxBatchTime}]")
        }
        flushEventBuffer()
      } else {
        // Send all the events in the buffer in up to maxBatchTime even if no more events are added to the buffer
        setScheduleBufferFlush()
      }
  }

  def isAfterMaxBatchTime(): Boolean = System.currentTimeMillis() - lastSendTime > maxBatchTime

  def isSendScheduled(): Boolean = !(scheduledSend.isDone || scheduledSend.isCancelled)

  def scheduleBufferFlush(): Unit = {
    scheduledSend.cancel(false)
    scheduledSend = exec.schedule(eventBufferFlusher, maxBatchTime, TimeUnit.MILLISECONDS)
  }

  def setScheduleBufferFlush(): Unit = {
    if (!isSendScheduled()) {
      if(debug){
        logger.log(Level.WARNING, s"[Moesif] Scheduler is set for ${maxBatchTime/1000} seconds later...")
      }
      scheduleBufferFlush()
    }
  }

  def cancelScheduleBufferFlush(): Unit = {
    if (isSendScheduled()) {
      if(debug){
        logger.log(Level.WARNING, "[Moesif] Cancelling schedule to flush events")
      }
      scheduledSend.cancel(false)
    }
  }

  def addBackEvents(sendingEvents: Seq[EventModel]): Unit = {
    if (maxApiEventsToHoldInMemory >= eventModelBuffer.size + sendingEvents.size) {
      eventModelBuffer ++= sendingEvents
    } else {
      logger.log(Level.WARNING, s"[Moesif] ${sendingEvents.size} Skipped Events due to event buffer size [${eventModelBuffer.size}] is over max ApiEventsToHoldInMemory ${maxApiEventsToHoldInMemory}")
    }
  }

  def flushEventBuffer(): Unit = synchronized {
    if (eventModelBuffer.nonEmpty) {
      val eventModelCache: mutable.ArrayBuffer[EventModel] = eventModelBuffer.clone()
      eventModelBuffer.clear()
      lastSendTime = System.currentTimeMillis()

      while(eventModelCache.nonEmpty){
        val sendingEvents: Seq[EventModel] = eventModelCache.take(batchSize)
        eventModelCache --= sendingEvents

        val companyIds = Try(sendingEvents.map(_.getCompanyId).distinct.toString()).getOrElse("EMPTY COMPANY")

        val callBack: APICallBack[HttpResponse] = new APICallBack[HttpResponse] {
          def onSuccess(context: HttpContext, response: HttpResponse): Unit = {
            if (context.getResponse.getStatusCode != 201) {
              logger.log(Level.WARNING, s"[Moesif] server returned status:${context.getResponse.getStatusCode} while sending API events [${sendingEvents.size}/${batchSize}]")
              addBackEvents(sendingEvents)
              setScheduleBufferFlush()
            }
            else {
              logger.log(Level.INFO, s"[Moesif] sent [${sendingEvents.size}/${batchSize}] events successfully | queue size: [${eventModelBuffer.size}/$maxApiEventsToHoldInMemory]")
              // if this was called while a scheduled send task was still live, cancel it because we just sent
              cancelScheduleBufferFlush()
              setScheduleBufferFlush()
            }
          }
          def onFailure(context: HttpContext, ex: Throwable): Unit = {
            logger.log(Level.WARNING, s"[Moesif] failed to send API events [flushSize: ${sendingEvents.size}/${batchSize}] [ArrayBuffer size: ${eventModelBuffer.size}/${maxApiEventsToHoldInMemory}] [company ids: ${companyIds}] to Moesif: ${ex.getMessage}", ex)
            addBackEvents(sendingEvents)
            setScheduleBufferFlush()
          }
        }

        val events = sendingEvents.asJava
        moesifApi.createEventsBatchAsync(events, callBack, useGzip)
      }
    }
  }

  /**
    * Internal helper class, inspired by
    * http://doc.akka.io/docs/akka/2.4/scala/stream/stream-cookbook.html#Calculating_the_digest_of_a_ByteString_stream
    *
    * @param byteArray to store the incomoing bytes
    */
  class FullBodyFilterGraphStage(byteArray: mutable.ArrayBuilder[Byte]) extends GraphStage[FlowShape[ByteString, ByteString]] {
    val flow = Flow.fromFunction[ByteString, ByteString](identity)
    override val shape = flow.shape
    val in = shape.in
    val out = shape.out

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val chunk = grab(in)
          byteArray ++= chunk.toArray
          push(out, chunk)
        }
      })
    }
  }
}


/**
  * The moesif api filter module.
  */
class MoesifApiFilterModule
  extends SimpleModule(
    bind[MoesifApiFilterConfig].toProvider[MoesifApiFilterConfigProvider],
    bind[MoesifApiFilter].toSelf
  )

/**
  * Moesif Api filter components.
  */
trait MoesifApiFilterComponents {
  def configuration: Configuration
  def materializer: Materializer

  lazy val moesifApiFilterConfig: MoesifApiFilterConfig = MoesifApiFilterConfig.fromConfiguration(configuration)
  lazy val moesifApiFilter: MoesifApiFilter             = new MoesifApiFilter(moesifApiFilterConfig)(materializer)
}

object MoesifApiFilter{
  def buildUriHelper(host: String, uri: String, secure: Boolean): String = {
    if (uri.contains("://")) {
      uri
    }
    else {
      val protocol = if (secure) "https://" else "http://"

      // Add "/" if requestHeader.uri(route) is not start with "/"
      val route = if (uri.startsWith("/")) uri else "/" + uri

      protocol + host + route
    }
  }

  def buildUri(requestHeader: RequestHeader): String = {
    buildUriHelper(requestHeader.host, requestHeader.uri, requestHeader.secure)
  }

}
