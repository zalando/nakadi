package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database._
import play.api.libs.iteratee.Enumerator
import play.api.libs.json._
import play.api.mvc._

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

object RootController extends Controller {

  // import controllers.Serializers._

  def metrics() = Action {
    Ok(Json.toJson(Metrics.currentMetrics))
  }
}


