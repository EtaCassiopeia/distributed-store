import com.ning.http.client.{AsyncHttpClientConfig, AsyncHttpClient}
import org.specs2.mutable.{Specification, Tags}
import play.api.libs.json._

trait Metric {
  val name: String
}

case class Dummy(name: String) extends Metric
case class GaugeMetric(name: String) extends Metric //gauge = value
case class CounterMetric(name: String) extends Metric //type counter = count && !mean && !m1_rate && !p50
case class HistogramMetric(name: String) extends Metric //type histogram = count && mean && p50 && !m1_rate
case class MeterMetric(name: String) extends Metric //type meter = count && !mean && !p50 && m1_rate
case class TimerMetric(name: String) extends Metric //type timer = count && mean && p50 && m1_rate

object Kibana {

  private[this] val config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder()
    .setAllowPoolingConnection(true)
    .setCompressionEnabled(true)
    .setRequestTimeoutInMs(60000)
    .setIdleConnectionInPoolTimeoutInMs(60000)
    .setIdleConnectionTimeoutInMs(60000)
    .build()

  private[this] val httpClient: AsyncHttpClient = new AsyncHttpClient(config)

  def findMetrics(index: String): Seq[Metric] = {
    val body = httpClient.preparePost(s"http://localhost:9200/$index/_search").setBody(Json.stringify(
      Json.obj(
        "query" -> Json.obj(
          "query_string" -> Json.obj(
            "query" -> "*"
          )
        )
      )
    )).execute().get().getResponseBody
    val result = Json.parse(body) \ "hits" \ "hits"
    result.as[JsArray].value.map(_.as[JsObject]).map { obj =>
      val name = (obj \ "_source" \ "name").as[String]
      (obj \ "_type").as[String] match {
        case "gauge" => new GaugeMetric(name)
        case "counter" => new CounterMetric(name)
        case "histogram" => new HistogramMetric(name)
        case "meter" => new MeterMetric(name)
        case "timer" => new TimerMetric(name)
        case _ => new Dummy(name)
      }
    }
  }

  def toQuery(metrics: Seq[Metric]) = {
    var list = Json.obj()
    val ids = JsArray(metrics.zipWithIndex.map(t => JsNumber(t._2)))
    metrics.zipWithIndex.foreach { tuple =>
      val i = tuple._2
      val name = tuple._1.name
      list = list ++ Json.obj(s"$i" -> Json.obj(
        "query" -> s"""name:"$name"""",
        "alias" -> name,
        "color"-> "#7EB26D",
        "id" -> i,
        "pin" -> false,
        "type" -> "lucene",
        "enable" -> true
      ))
    }
    Json.obj(
      "list" -> list,
      "ids" -> ids
    )
  }
}


class KibanaSpec extends Specification with Tags {
  sequential

  "Kibana API" should {

    "Generate dashboards" in {
      val metrics = Kibana.findMetrics("blah-2014-07")
      println(Json.prettyPrint(Kibana.toQuery(metrics)))
      success
    }
  }
}