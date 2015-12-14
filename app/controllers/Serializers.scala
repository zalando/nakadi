package controllers

import database.CustomTypes.{Name, Namespace, OID}
import database.{Database, DbType, StoredProcedure}
import play.api.libs.json._

// mostly these are to avoid having to cast the keys to string (in which case
// the built in Writers work ok. Feels like there should be an easier way.
object Serializers {
/*
  implicit val mapOidDbType = new Writes[Map[OID, DbType]] {
    override def writes(o: Map[OID, DbType]): JsValue = {
      JsObject {
        o.map {
          case (oid, dbType) => oid.toString -> Json.toJson(dbType)
        }.toSeq
      }
    }
  }

  implicit val writeNameToSprocs = new Writes[Map[(Namespace, Name), Seq[StoredProcedure]]] {
    override def writes(sprocs: Map[(Namespace, Name), Seq[StoredProcedure]]): JsValue = {
      JsObject {
        sprocs.map {
          case (key, procs) => key.toString -> Json.toJson(procs)
        }.toSeq
      }
    }
  }

  implicit val writeDbToTypesMap = new Writes[Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]]] {
    override def writes(o: Map[Database, Map[(Namespace, Name), Seq[StoredProcedure]]]): JsValue = {
      JsObject {
        o.map {
          case (database, sprocs) => database.name -> Json.toJson(sprocs)
        }.toSeq
      }
    }
  }

  implicit val writeDbtoDbTypes = new Writes[Map[Database, Map[OID, DbType]]] {
    override def writes(o: Map[Database, Map[OID, DbType]]): JsValue = {
      JsObject {
        o.map {
          case (db, map) => db.name -> Json.toJson(map)
        }.toSeq
      }
    }
  }
  */
}
