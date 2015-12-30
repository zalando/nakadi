package de.zalando.nakadi.utils

import java.nio.charset.StandardCharsets
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

trait KeyDeserializer[K] extends Deserializer[K]
trait ValueDeserializer[V] extends Deserializer[V]

sealed trait UTF8StringDeserializer{
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  def close(): Unit = ()
  def deserialize(topic: String, data: Array[Byte]): String = new String(data, StandardCharsets.UTF_8)
}

final class UTF8StringKeyDeserializer extends KeyDeserializer[String] with UTF8StringDeserializer
final class UTF8StringValueDeserializer extends ValueDeserializer[String] with UTF8StringDeserializer

trait KeySerializer[K] extends Serializer[K]
trait ValueSerializer[V] extends Serializer[V]

sealed trait UTF8StringSerializer{
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  def close(): Unit = ()
  def serialize(topic: String, data: String): Array[Byte] = if (data == null) null else data.getBytes(StandardCharsets.UTF_8)
}

final class UTF8StringKeySerializer extends KeySerializer[String] with UTF8StringSerializer
final class UTF8StringValueSerializer extends ValueSerializer[String] with UTF8StringSerializer
