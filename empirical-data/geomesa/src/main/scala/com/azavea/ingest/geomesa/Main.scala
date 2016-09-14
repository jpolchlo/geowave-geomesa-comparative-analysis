package com.azavea.ingest.geomesa

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.geotools.feature.simple._
import org.opengis.feature.simple._

//import geotrellis.spark.util.SparkUtils

import com.azavea.ingest.common._
import com.azavea.ingest.common.csv.HydrateRDD._
import com.azavea.ingest.common.shp.HydrateRDD._

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

object Main {
  def centerReader(location: String) = {
    val file = new File(location)

    val inputStream = 
      if (file.exists) {
        // local file
        new FileInputStream(file)
      } else {
        val urlMaybe = Try(new java.net.URL(location))
        if (urlMaybe.isSuccess) {
          // web-hosted file
          urlMaybe.get.openStream
        } else {
          // s3 file?
          val provider = new DefaultAWSCredentialsProviderChain
          val client = new AmazonS3Client(provider)
          val s3uri = new AmazonS3URI(location)
          val s3streamMaybe = Try(client.getUrl(s3uri.getBucket, s3uri.getKey).openStream)
          if (s3streamMaybe.isSuccess)
            s3streamMaybe.get
          else
            throw new IllegalArgumentException(s"Resource at $location is not valid")
        }
      }

    new BufferedReader(new InputStreamReader(inputStream))
  }

  def translateIngest(params: Ingest.Params, src: RDD[SimpleFeature])(implicit sc: SparkContext) = {
    // if (!params.centerFile.exists || !params.centerFile.isFile)
    //   throw new java.lang.IllegalArgumentException("Center file must exist")

    // val brMaybe = Try(new BufferedReader(new InputStreamReader(new FileInputStream(params.centerFile))))
    // if (brMaybe.isFailure)
    //   throw new java.io.IOException("Problem loading center file")

    val br = centerReader(params.centerFile) 
    val iter = new java.util.Iterator[String] { 
      def next() = br.readLine 
      def hasNext() = br.ready 
      def remove() = throw new UnsupportedOperationException 
    }
    val centers = sc.parallelize(iter.map{ line => ({ arr: Array[String] => (arr(0).toDouble, arr(1).toDouble) })(line.split("\t")) }
                                     .take(params.numReplicates)
                                     .toSeq)
    br.close

    Ingest.shiftAndIngest(params)(centers.cartesian(src))
  }

  def main(args: Array[String]): Unit = {
    val params = CommandLine.parser.parse(args, Ingest.Params()) match {
      case Some(p) => p
      case None => throw new Exception("Problem parsing command line options, see terminal output for details")
    }

    val conf: SparkConf =
      new SparkConf()
        .setAppName("GeoMesa ingest utility")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(conf)

    params.csvOrShp match {
      case Ingest.SHP => {
        val urls = getShpUrls(params.s3bucket, params.s3prefix)
        val shpUrlRdd = shpUrlsToRdd(urls)
        val shpSimpleFeatureRdd: RDD[SimpleFeature] = NormalizeRDD.normalizeFeatureName(shpUrlRdd, params.featureName)

        Ingest.registerSFT(params)(shpSimpleFeatureRdd.first.getType)
        Ingest.ingestRDD(params)(shpSimpleFeatureRdd)
      }
      case Ingest.CSV => {
        val urls = getCsvUrls(params.s3bucket, params.s3prefix, params.csvExtension)
        val tybuilder = new SimpleFeatureTypeBuilder
        tybuilder.setName(params.featureName)
        params.codec.genSFT(tybuilder)
        val sft = tybuilder.buildFeatureType
        val csvRdd: RDD[SimpleFeature] = csvUrlsToRdd(urls, params.featureName, params.codec, params.dropLines, params.separator)

        Ingest.registerSFT(params)(sft)
        Ingest.ingestRDD(params)(csvRdd)
      }
    }
  }
}

