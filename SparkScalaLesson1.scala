package learning

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql._


case class AddressRawData(
                           addressId: String,
                           customerId: String,
                           address: String
                         )

case class AddressData(
                        addressId: String,
                        customerId: String,
                        address: String,
                        number: Option[Int],
                        road: Option[String],
                        city: Option[String],
                        country: Option[String]
                      )

object SparkScalaLesson1 {

  def main(args: Array[String]) {
    //    val spark = SparkSession.builder.master("local[*]").appName("SparkScalaLesson1").getOrCreate()
    val spark = SparkSession.builder().config("spark.driver.memory", "1g").config("spark.testing.memory", "2147480000").master("local[2]").appName("AccountAssignment").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    val addressDF: DataFrame = spark.read.option("header", "true").csv("src/main/resources/address_data.csv")
    //    addressDF.printSchema()
    val addressDS: Dataset[AddressRawData] = addressDF.as[AddressRawData]

    def addressParser(unparsedAddress: Seq[AddressData]): Seq[AddressData] = {
      unparsedAddress.map(address => {
          val split = address.address.split(", ")

        address.copy(
          number = Some(split(0).toInt),
          road = Some(split(1)),
          city = Some(split(2)),
          country = Some(split(3))
        )
      }

      )
    }

    addressDS.show()
    val addressDS1: Dataset[AddressData] = addressDS.as[AddressRawData].map {
      row =>
        AddressData(row.addressId, row.customerId, row.address, null, null, null, null
        )
    }


    addressDS1.show()

//    addressDS1.as[AddressData].map(row => {
//      println(row(2))
//
//    })Encoders.product[AddressData]
//
//
//    val addressParser = udf(addressParser _)
//
//    addressDF.select(addressParser($"address"))


    def getFormattedName(contentName : String, titleVersionDesc:String): Option[String] = {
      Option(contentName+titleVersionDesc)
    }

    val get_formatted_name = udf(getFormattedName _)

    addressDF.select(get_formatted_name($"address", $"addressId")).show(false)


      spark.stop()
  }
}