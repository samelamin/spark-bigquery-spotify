/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.spark
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.sql.types.StructType
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryOutputFormat}
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql._
import com.google.cloud.hadoop.io.bigquery._

package object bigquery {

  object CreateDisposition extends Enumeration {
    val CREATE_IF_NEEDED, CREATE_NEVER = Value
  }

  object WriteDisposition extends Enumeration {
    val WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY = Value
  }

  /**
   * Enhanced version of [[SQLContext]] with BigQuery support.
   */
  implicit class BigQuerySQLContext(self: SQLContext) {

    val sc = self.sparkContext
    val conf = sc.hadoopConfiguration
    val STAGING_DATASET_LOCATION = "bq.staging_dataset.location"

    // Register GCS implementation
    if (conf.get("fs.gs.impl") == null) {
      conf.set("fs.gs.impl", classOf[GoogleHadoopFileSystem].getName)
    }

    /**
     * Set GCP project ID for BigQuery.
     */
    def setBigQueryProjectId(projectId: String): Unit = {
      conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)

      // Also set project ID for GCS connector
      if (conf.get("fs.gs.project.id") == null) {
        conf.set("fs.gs.project.id", projectId)
      }
    }

    /**
     * Set GCS bucket for temporary BigQuery files.
     */
    def setBigQueryGcsBucket(gcsBucket: String): Unit =
      conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, gcsBucket)

    /**
     * Set BigQuery dataset location, e.g. US, EU.
     */
    def setBigQueryDatasetLocation(location: String): Unit =
      conf.set(STAGING_DATASET_LOCATION, location)

    /**
     * Set GCP JSON key file.
     */
    def setGcpJsonKeyFile(jsonKeyFile: String): Unit = {
      conf.set("mapred.bq.auth.service.account.json.keyfile", jsonKeyFile)
      conf.set("fs.gs.auth.service.account.json.keyfile", jsonKeyFile)
    }

    /**
     * Set GCP pk12 key file.
     */
    def setGcpPk12KeyFile(pk12KeyFile: String): Unit = {
      conf.set("google.cloud.auth.service.account.keyfile", pk12KeyFile)
      conf.set("mapred.bq.auth.service.account.keyfile", pk12KeyFile)
      conf.set("fs.gs.auth.service.account.keyfile", pk12KeyFile)
    }

    /**
     * Perform a BigQuery SELECT query and load results as a [[DataFrame]].
      *
      * @param sqlQuery SQL query in SQL-2011 dialect.
     */

    /**
     * Load a BigQuery table as a [[DataFrame]].
     */
    def bigQueryTable(tableRef: TableReference): DataFrame = {
      val fullyQualifiedInputTableId = BigQueryStrings.toString(tableRef)
      BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

      val tableData = sc.newAPIHadoopRDD(
        conf,
        classOf[GsonBigQueryInputFormat],
        classOf[LongWritable],
        classOf[JsonObject]).map(_._2.toString)

      val df = self.read.json(tableData)
      df
    }

    /**
     * Load a BigQuery table as a [[DataFrame]].
     */
    def bigQueryTable(tableSpec: String): DataFrame =
      bigQueryTable(BigQueryStrings.parseTableReference(tableSpec))

  }

  /**
   * Enhanced version of [[DataFrame]] with BigQuery support.
   */
  implicit class BigQueryDataFrame(self: DataFrame) extends Serializable {

    val sqlContext = self.sqlContext
    @transient
    val conf = sqlContext.sparkContext.hadoopConfiguration
    @transient
    lazy val jsonParser = new JsonParser()
    val adaptedDf = BigQueryAdapter(self)
    /**
     * Save a [[DataFrame]] to a BigQuery table.
     */
    def saveAsBigQueryTable(tableSpec: String,
                            writeDisposition: WriteDisposition.Value = null,
                            createDisposition: CreateDisposition.Value = null,
                            isPartitionedByDay: Boolean = false): Unit = {
      val tableSchema = BigQuerySchema(adaptedDf)

      BigQueryConfiguration.configureBigQueryOutput(conf, tableSpec, tableSchema)
      conf.set("mapreduce.job.outputformat.class", classOf[BigQueryOutputFormat[_, _]].getName)

      adaptedDf
        .toJSON
        .rdd
        .map(json => (null, jsonParser.parse(json)))
        .saveAsNewAPIHadoopDataset(conf)
    }

  }

}
