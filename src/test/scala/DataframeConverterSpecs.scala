import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class DataframeConverterSpecs extends FeatureSpec with GivenWhenThen with DataFrameSuiteBase {
  feature("Schema Converters") {
    scenario("When converting a dataframe schema to BQ TableSchema") {
      Given("A dataframe schema")

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


      print("*********** test me")
      val count = df.count()
      df.schema should not be null

    }
  }
}