// Databricks notebook source
//standard avroschema for eventhub
val avroSchema = new org.apache.avro.Schema.Parser().parse("""{"type":"record","name":"EventData","namespace":"Microsoft.ServiceBus.Messaging","fields":[{"name":"SequenceNumber","type":"long"},{"name":"Offset","type":"string"},{"name":"EnqueuedTimeUtc","type":"string"},{"name":"SystemProperties","type":{"type":"map","values":["long","double","string","bytes"]}},{"name":"Properties","type":{"type":"map","values":["long","double","string","bytes","null"]}},{"name":"Body","type":["null","bytes"]}]}""");

// COMMAND ----------

import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.types._

//generate spark schema from avro schema
var sparkAvroSchema : StructType = new StructType
import scala.collection.JavaConversions._     
 for(field <- avroSchema.getFields()){
  sparkAvroSchema = sparkAvroSchema.add(field.name, SchemaConverters.toSqlType(field.schema).dataType)
 }

// COMMAND ----------

//location of incoming avro data. Should use secrets
spark.conf.set( "fs.azure.account.key.inputaccount.blob.core.windows.net", "blobkey==")

// COMMAND ----------

////location of output parquet data. Should use secrets
spark.conf.set( "fs.azure.account.key.outputaccount.blob.core.windows.net", "blobkey==")

// COMMAND ----------

//read the data. Should use secrets for key
val streamingDF = spark.readStream 
                    .format("abs-aqs") 
                    .option("fileFormat", "avro") 
                    .option("queueName", "weather-queue") 
                    .option("connectionString", "DefaultEndpointsProtocol=https;AccountName=blobaccountforqueue;AccountKey=your-key;EndpointSuffix=core.windows.net") 
                    .schema(sparkAvroSchema) 
                    .load()

// COMMAND ----------

import org.apache.spark.sql.types._

//actual data schema as the payload is json
val jsonSchema = StructType(
  Seq(
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true),
    StructField("timezone", StringType, true),
    StructField("currently", StructType(
      Seq (
        StructField("time", LongType, true),
        StructField("summary", StringType, true),
        StructField("icon", StringType, true),
        StructField("nearestStormDistance", LongType, true),
        StructField("nearestStormBearing", LongType, true),
        StructField("precipIntensity", LongType, true),
        StructField("precipProbability", LongType, true),
        StructField("temperature", DoubleType, true),
        StructField("apparentTemperature", DoubleType, true),
        StructField("dewPoint", DoubleType, true),
        StructField("humidity", DoubleType, true),
        StructField("pressure", DoubleType, true),
        StructField("windSpeed", DoubleType, true),
        StructField("windGust", DoubleType, true),
        StructField("windBearing", DoubleType, true),
        StructField("cloudCover", DoubleType, true),
        StructField("uvIndex", DoubleType, true),
        StructField("visibility", DoubleType, true),
        StructField("ozone", DoubleType, true)
        )
      ), true),
      StructField("flags", StructType(
          Seq(
            StructField("sources", ArrayType(StringType, true), true),
            StructField("nearest-station", DoubleType, true),
            StructField("units", StringType, true)
          )
        ), true),
      StructField("offset", LongType, true)
   )
)

// COMMAND ----------

//parset and flatten the data
import org.apache.spark.sql.functions._
val parsedData = streamingDF.select(from_json(col("Body").cast("string"), jsonSchema).alias("weatherData"))
                 .selectExpr("weatherData.*")
                 .select(col("latitude"), col("longitude"), col("timezone"), col("currently.*"), col("flags.*"), col("offset"))
                 .withColumn("weatherTime", from_unixtime(col("time")).cast(TimestampType))

// COMMAND ----------

//write the output
parsedData.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "wasbs://weather-transformed@outputblob.blob.core.windows.net/checkpoint1/")
  .option("path", "wasbs://weather-transformed@outputblob.blob.core.windows.net/processed/")
  .start()
