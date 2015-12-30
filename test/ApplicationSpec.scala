import de.zalando.nakadi.models.{Partition, Topic}
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.test._
import play.api.test.Helpers._

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends Specification {

  "Application" should {

    "send 404 on a bad request" in new WithApplication{
      route(FakeRequest(GET, "/boum")) must beSome.which (status(_) == NOT_FOUND)
    }

    "get metrics" in new WithApplication{
      val metrics = route(FakeRequest(GET, "/metrics")).get

      status(metrics) must equalTo(OK)
      contentType(metrics) must beSome.which(_ == "application/json")
    }

    "get topics" in new WithApplication{
      val topics = route(FakeRequest(GET, "/topics")).get

      status(topics) must equalTo(OK)
      contentType(topics) must beSome.which(_ == "application/json")
      contentAsJson(topics).as[Seq[Topic]] must contain( (t: Topic) => t.name must beEqualTo("test-topic") )
    }

    "get partitions" in new WithApplication{
      val partitions = route(FakeRequest(GET, s"/topics/test-topic/partitions")).get
      status(partitions) must equalTo(OK)
      contentType(partitions) must beSome.which(_ == "application/json")
      contentAsJson(partitions).as[Seq[Partition]] must have size (2)
    }
  }
}
