package com.spotify.spark.bigquery


import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
  * Created by root on 14/12/2016.
  */
object SchemaConverter {

  def dfToBQSchema(schema: StructType): TableSchema = {
    val schemaFields = schema.fields.map { field =>
      val fieldType = field.dataType match {
        case ByteType | ShortType | IntegerType | LongType => "INTEGER"
        case FloatType | DoubleType => "FLOAT"
        case _: DecimalType | StringType => "STRING"
        case BinaryType => "BYTES"
        case BooleanType => "BOOLEAN"
        case TimestampType => "TIMESTAMP"
        case ArrayType(_, _) | MapType(_, _, _) | _: StructType => "RECORD"
      }
      new TableFieldSchema().setName(field.name).setType(fieldType)
    }.toList.asJava

    val tableSchema = new TableSchema().setFields(schemaFields)
    tableSchema
  }

}
