package com.spotify.spark.bigquery

import org.apache.spark.sql.types.{StructType, _}

/**
  * Created by root on 12/15/16.
  */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}

/**
  * Builds BigQuery input JSON schema based on DataFrame.
  * Example schema can be found here: https://cloud.google.com/bigquery/docs/personsDataSchema.json
  */
object BigQuerySchema {

  private def getMode(field: StructField) = {
    field.dataType match {
      case ArrayType(_, _) => "REPEATED"
      case _ => if (field.nullable) "NULLABLE" else "REQUIRED"
    }
  }

  private def getTypeName(dataType: DataType) = {
    dataType match {
      case ByteType | ShortType | IntegerType | LongType => "INTEGER"
      case FloatType | DoubleType => "FLOAT"
      case _: DecimalType | StringType => "STRING"
      case BinaryType => "BYTES"
      case BooleanType => "BOOLEAN"
      case TimestampType => "TIMESTAMP"
      case ArrayType(_, _) | MapType(_, _, _) | _: StructType => "RECORD"
    }
  }

  private def typeToJson(field: StructField, dataType: DataType): JValue = {
    dataType match {
      case structType: StructType =>
        ("type" -> getTypeName(dataType)) ~
          ("fields" -> structType.fields.map(fieldToJson(_)).toList)
      case arrayType: ArrayType =>
        arrayType.elementType match {
          case _: ArrayType =>
            throw new IllegalArgumentException(s"Multidimensional arrays are not supported: ${field.name}")
          case other =>
            typeToJson(field, other)
        }
      case mapType: MapType =>
        throw new IllegalArgumentException(s"Unsupported type: ${dataType}")
      case other =>
        ("type" -> getTypeName(dataType))
    }
  }

  private def fieldToJson(field: StructField): JValue = {
    ("name" -> field.name) ~
      ("mode" -> getMode(field)) merge
      typeToJson(field, field.dataType)
  }

  def apply(df: DataFrame): String = {
    pretty(render(JArray(df.schema.fields.map(fieldToJson(_)).toList)))
  }
}

