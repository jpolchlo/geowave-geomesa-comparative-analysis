package com.azavea.ingest.geomesa

import com.azavea.ingest.common._

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.geotools.feature.simple._
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.opengis.feature.simple._

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
      case None => throw new Exception("provide the right arguments, ya goof")
    }

    val sparkConf: SparkConf = (new SparkConf).setAppName("GeoMesa Ingest Utility")
                                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    implicit val sc: SparkContext = new SparkContext(sparkConf)

    params.csvOrShp match {
      case Ingest.SHP => {
        val urls = HydrateRDD.getShpUrls(params.s3bucket, params.s3prefix)
        val shpRdd: RDD[SimpleFeature] = HydrateRDD.normalizeShpRdd(HydrateRDD.shpUrls2shpRdd(urls), params.featureName)

        Ingest.registerSFT(params)(shpRdd.first.getType)

        if (params.translate) {
          translateIngest(params, shpRdd)
        } else {
          Ingest.ingestRDD(params)(shpRdd)
        }
      }
      case Ingest.CSV => {
        val urls = HydrateRDD.getCsvUrls(params.s3bucket, params.s3prefix, params.csvExtension)
        val tybuilder = new SimpleFeatureTypeBuilder
        tybuilder.setName(params.featureName)
        params.codec.genSFT(tybuilder)
        val sft = tybuilder.buildFeatureType
        val builder = new SimpleFeatureBuilder(sft)
        val csvRdd: RDD[SimpleFeature] = HydrateRDD.csvUrls2Rdd(urls, params.featureName, params.codec, params.dropLines, params.separator)

        Ingest.registerSFT(params)(sft)

        if (params.translate) {
          translateIngest(params, csvRdd)
        } else {
          Ingest.ingestRDD(params)(csvRdd)
        }
      }
    }
  }
}
