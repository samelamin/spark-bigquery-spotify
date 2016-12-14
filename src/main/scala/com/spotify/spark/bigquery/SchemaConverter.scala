package com.spotify.spark.bigquery

import com.google.api.services.bigquery.model.TableSchema

/**
  * Created by root on 14/12/2016.
  */
object SchemaConverter {

  def convertDataframeSchemaToTableSchema(schema: String): TableSchema = {
    val tableSchema = new TableSchema()

    tableSchema
  }

}
