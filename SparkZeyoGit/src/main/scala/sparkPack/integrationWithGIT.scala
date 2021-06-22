package sparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//for importing col function 
import java.io.File

object integrationWithGIT {
  
   def main(args:Array[String]):Unit={

		val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
				val sc= new SparkContext(conf)
				sc.setLogLevel("ERROR")

				val spark= SparkSession.builder().getOrCreate()
				import spark.implicits._
  
				
					println("====================csv read=================================")
			
			val df = spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
		
		df.printSchema()
					
			df.show()
			
			
			println("=======================inferSchema=============================")
			
			val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///C:/data/usdata.csv")
			
			df1.printSchema()
			df1.show()
			
			println("=======================DFtoAvro=============================")
			
		df1.write.format("com.databricks.spark.avro").mode("Ignore").save("file:///C:/data/avro_data_Task20June")
		
			println("avro data written")

					
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
				
   } 
}