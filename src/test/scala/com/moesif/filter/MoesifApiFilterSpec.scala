package com.moesif.filter

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.moesif.api.http.client.{APICallBack, HttpContext}
import com.moesif.api.http.response.HttpResponse
import com.moesif.api.models._
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import org.scalatestplus.mockito.MockitoSugar
import play.api.mvc._
import play.api.test.Helpers._
import play.api.test._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import akka.util.ByteString
import play.api.libs.streams.Accumulator

import java.util.Date

class MoesifApiFilterSpec extends WordSpec
  with MustMatchers
  with MockitoSugar
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with OneInstancePerTest
  with ScalaFutures {

  implicit val system = ActorSystem()
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val defaultPatience = PatienceConfig(timeout = 5.seconds, interval = 100.millis)

  override def afterAll(): Unit = {
    system.terminate()
  }

  "MoesifApiFilter" must {
    "correctly build URI for secure requests" in {
      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(HOST -> "example.com")

      val uri = MoesifApiFilter.buildUriHelper("example.com", "/test", true)
      uri must be("https://example.com/test")
    }

    "correctly build URI for non-secure requests" in {
      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(HOST -> "example.com")

      val uri = MoesifApiFilter.buildUriHelper("example.com", "/test", false)
      uri must be("http://example.com/test")
    }

    "not modify URI if it already contains protocol" in {
      val requestHeader = FakeRequest("GET", "https://external.com/test")
        .withHeaders(HOST -> "example.com")

      val uri = MoesifApiFilter.buildUri(requestHeader)
      uri must be("https://external.com/test")
    }

    "correctly handle request body processing when enabled" in {
      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(50)
      when(config.resBodySizeLimit).thenReturn(50)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]

      val body = ByteString("""{"test": "data"}""")
      val requestHeader = FakeRequest("POST", "/test")
        .withHeaders(
          CONTENT_TYPE -> "application/json",
          "X-Test-Header" -> "test-value",
          CONTENT_LENGTH -> body.length.toString
      )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok("response")
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run(body)

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event: EventModel = events.head
        event.getRequest must not be(null)
        event.getRequest.getBody must not be(null)
      }
    }

    "correctly handle request body processing when disabled" in {
      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(false)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(50)
      when(config.resBodySizeLimit).thenReturn(50)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")
      val requestHeader = FakeRequest("POST", "/test")
        .withHeaders(
          CONTENT_TYPE -> "application/json",
          "X-Test-Header" -> "test-value",
          CONTENT_LENGTH -> body.length.toString
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok("response")
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run(body)

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getRequest must not be(null)
        event.getRequest.getBody must be(null)
      }
    }

    "correctly handle response body processing when enabled" in {
      val config = mock[MoesifApiFilterConfig]
      when(config.responseBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(200)
      when(config.resBodySizeLimit).thenReturn(200)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")
      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value"
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok(body)
            .withHeaders(
              CONTENT_TYPE -> "application/json",
              CONTENT_LENGTH -> body.length.toString
            )
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)
        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getResponse must not be(null)
        event.getResponse.getBody must not be(null)
      }

    }

    "correctly handle response body processing when disabled" in {
      val config = mock[MoesifApiFilterConfig]
      when(config.responseBodyProcessingEnabled).thenReturn(false)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(50)
      when(config.resBodySizeLimit).thenReturn(50)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")
      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value"
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok(body)
              .withHeaders(
                CONTENT_TYPE -> "application/json",
                CONTENT_LENGTH -> body.length.toString
              )
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getResponse must not be(null)
        event.getResponse.getBody must be(null)
      }
    }

    "correctly handle request body size limit is high" in {

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")

      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(true)
      when(config.responseBodyProcessingEnabled).thenReturn(false)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(body.length - 10)
      when(config.resBodySizeLimit).thenReturn(200)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]


      val requestHeader = FakeRequest("POST", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value",
          CONTENT_LENGTH -> body.length.toString
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok("{}")
              .withHeaders(CONTENT_TYPE -> "application/json")

          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getRequest must not be(null)
        event.getRequest.getBody must be(null)
      }
    }

    "correctly handle response body size limit is high" in {

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")

      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(false)
      when(config.responseBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(200)
      when(config.resBodySizeLimit).thenReturn(body.length - 10)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]


      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value"
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok(body)
              .withHeaders(
                CONTENT_TYPE -> "application/json",
                CONTENT_LENGTH -> body.length.toString
              )
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getResponse must not be(null)
        event.getResponse.getBody must be(null)
      }
    }

    "correctly handle request body size limit is high without content length" in {

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")

      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(true)
      when(config.responseBodyProcessingEnabled).thenReturn(false)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(body.length - 10)
      when(config.resBodySizeLimit).thenReturn(200)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]


      val requestHeader = FakeRequest("POST", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value"
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok("{}")
              .withHeaders(CONTENT_TYPE -> "application/json")

          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getRequest must not be(null)
        event.getRequest.getBody must be(null)
      }
    }

    "correctly handle response body size limit is high without content length" in {

      val body = ByteString("""{"data":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""")

      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(false)
      when(config.responseBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(1000)
      when(config.reqBodySizeLimit).thenReturn(200)
      when(config.resBodySizeLimit).thenReturn(body.length - 10)
      when(config.batchSize).thenReturn(25)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)
      val nextFilter = mock[EssentialAction]


      val requestHeader = FakeRequest("GET", "/test")
        .withHeaders(
          "X-Test-Header" -> "test-value"
        )
      when(nextFilter.apply(any[RequestHeader])).thenReturn(
        Accumulator.source[ByteString].mapFuture { source =>
          source.runFold(ByteString.empty)(_ ++ _).map { requestBody =>
            Results.Ok(body)
              .withHeaders(
                CONTENT_TYPE -> "application/json"
              )
          }
        }
      )

      val result = filter(nextFilter)(requestHeader).run()

      whenReady(result) { res =>
        res.header.status must be(OK)

        val events = filter.getEventBuffer();
        events.size mustBe 1
        val event = events.head
        event.getResponse must not be(null)
        event.getResponse.getBody must be(null)
      }
    }

    "respect maxApiEventsToHoldInMemory limits" in {
      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(5)
      when(config.reqBodySizeLimit).thenReturn(1000000)
      when(config.resBodySizeLimit).thenReturn(1000000)
      when(config.batchSize).thenReturn(10)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("https://api.moesif.net")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(2000000)

      val filter = new MoesifApiFilter(config)


      // Add multiple events and verify the buffer respects maxApiEventsToHoldInMemory
      (1 to 6).foreach { i =>
        val event = new EventModel()
        event.setRequest(new EventRequestBuilder().build())
        event.setResponse(new EventResponseBuilder().build())
        filter.sendEvent(event, MoesifAdvancedFilterConfiguration.getDefaultConfig())
      }

      // Allow some time for the scheduled flush to complete
      Thread.sleep(100)

      // Verify that the buffer size doesn't exceed maxApiEventsToHoldInMemory (5)
      val events = filter.getEventBuffer();
      events.size mustBe 5
    }

    "respect max batch size limits" ignore {

      val headers = Map(
        "X-Test-Header" -> "test-value",
        CONTENT_TYPE -> "application/json"
      ).asJava

      val eventRequest = new EventRequestBuilder().time(new Date()).
        verb("GET").
        apiVersion("1.0").
        headers(headers).
        build()
      val eventResponse = new EventResponseBuilder().
        time(new Date()).
        headers(headers).
        build()

      val event = new EventBuilder().
        request(eventRequest).
        response(eventResponse).
        build()

      val config = mock[MoesifApiFilterConfig]
      when(config.requestBodyProcessingEnabled).thenReturn(true)
      when(config.maxApiEventsToHoldInMemory).thenReturn(5)
      when(config.reqBodySizeLimit).thenReturn(1000000)
      when(config.resBodySizeLimit).thenReturn(1000000)
      when(config.batchSize).thenReturn(10)
      when(config.maxBatchTime).thenReturn(200000) // to avoid automatic flush during test
      when(config.moesifApplicationId).thenReturn("test-app-id")
      when(config.debug).thenReturn(false)
      when(config.moesifCollectorEndpoint).thenReturn("")
      when(config.useGzip).thenReturn(true)
      when(config.maxBatchSize).thenReturn(20)

      val filter = new MoesifApiFilter(config)

      val mockApi = mock[com.moesif.api.controllers.APIController]
      val field = filter.getClass.getDeclaredField("com$moesif$filter$MoesifApiFilter$$moesifApi")
      field.setAccessible(true)
      field.set(filter, mockApi)

      // Mock getSampleRateToUse to always return 100
      when(mockApi.getSampleRateToUse(any[EventModel])).thenReturn(100)

      // Mock createEventsBatchAsync to simulate successful API call
      doAnswer(new org.mockito.stubbing.Answer[Unit] {
        override def answer(invocation: org.mockito.invocation.InvocationOnMock): Unit = {
          val callback = invocation.getArguments()(1).asInstanceOf[APICallBack[HttpResponse]]
          val context = mock[HttpContext]
          val response = mock[HttpResponse]
          when(context.getResponse).thenReturn(response: HttpResponse)
          when(response.getStatusCode).thenReturn(201: Integer)
          callback.onSuccess(context, response)
        }
      }).when(mockApi).createEventsBatchAsync(any(), any(), any())

      filter.sendEvent(event, MoesifAdvancedFilterConfiguration.getDefaultConfig())
      filter.sendEvent(event, MoesifAdvancedFilterConfiguration.getDefaultConfig())
      filter.sendEvent(event, MoesifAdvancedFilterConfiguration.getDefaultConfig())

      // Allow some time for the scheduled flush to complete
      Thread.sleep(100)

      // Verify that the buffer size doesn't exceed maxApiEventsToHoldInMemory (5)
      var events = filter.getEventBuffer();
      events.size mustBe 3
      filter.flushEventBuffer();
      events = filter.getEventBuffer();
      events.size mustBe 2

      verify(mockApi).createEventsBatchAsync(any(), any(), eqTo(true))

    }

  }
}
