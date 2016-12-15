import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}
import com.spotify.spark.bigquery._
import org.apache.spark.sql.types.StructType

class DataframeConverterSpecs extends FeatureSpec with GivenWhenThen with DataFrameSuiteBase {
  feature("Schema Converters. Dataframe To BQ Schema") {
    scenario("When converting a simple dataframe") {
      Given("A dataframe")
      val sqlCtx = sqlContext
      val sampleJson = """{
                         |	"id": 1,
                         |	"error": null
                         |}""".stripMargin

      val df = sqlContext.read.json(sc.parallelize(List(sampleJson)))
      val dfSchema = df.schema

      When("Passing the schema to the converter")
      val tableSchema = SchemaConverter.dfToBQSchema(dfSchema)
      Then("We should receive a BQ Table Schema")
      tableSchema should not be null
      tableSchema.getFields should not be null
    }

    scenario("When converting a complex dataframe with nested data") {
      Given("A dataframe")
      val sqlCtx = sqlContext
      val sampleNestedJson = """{
                               |	"id": 1,
                               |	"error": null,
                               |	"result": {
                               |		"nPeople": 2,
                               |		"people": [{
                               |			"namePeople": "Inca",
                               |			"power": "1235",
                               |			"location": "asdfghjja",
                               |			"idPeople": 189,
                               |			"mainItems": "brownGem",
                               |			"verified": false,
                               |			"description": "Lorem impsum bla bla",
                               |			"linkAvatar": "avatar_12.jpg",
                               |			"longitude": 16.2434263,
                               |			"latitude": 89.355118
                               |		}, {
                               |			"namePeople": "Maya",
                               |			"power": "1235",
                               |			"location": "hcjkjhljhl",
                               |			"idPeople": 119,
                               |			"mainItems": "greenstone",
                               |			"verified": false,
                               |			"description": "Lorem impsum bla bla",
                               |			"linkAvatar": "avatar_6.jpg",
                               |			"longitude": 16.2434263,
                               |			"latitude": 89.3551185
                               |		}]
                               |	}
                               |}""".stripMargin

      val df = sqlContext.read.json(sc.parallelize(List(sampleNestedJson)))
      val dfSchema = df.schema

      When("Passing the schema to the converter")
      val tableSchema = SchemaConverter.dfToBQSchema(dfSchema)

      Then("We should receive a BQ Table Schema")
      tableSchema should not be null
      tableSchema.getFields should not be null
    }
  }
}