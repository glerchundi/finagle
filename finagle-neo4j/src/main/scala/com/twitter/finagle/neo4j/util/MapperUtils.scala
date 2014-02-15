package com.twitter.finagle.neo4j.util

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.{SerializationFeature, DeserializationFeature, ObjectMapper}
import com.twitter.util.Try
import com.fasterxml.jackson.annotation.JsonInclude

private[neo4j] object MapperUtils {

  val jsonMapper = new ObjectMapper()
  jsonMapper.registerModule(DefaultScalaModule)
  jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  jsonMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
  jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

  def readJson[T: ClassManifest](json: String): Try[T] =
    Try(
      jsonMapper
        .readValue(json, classManifest[T].erasure)
        .asInstanceOf[T]
    )

  def writeJson[T](value: AnyRef): Try[String] =
    Try(jsonMapper.writeValueAsString(value))

}
