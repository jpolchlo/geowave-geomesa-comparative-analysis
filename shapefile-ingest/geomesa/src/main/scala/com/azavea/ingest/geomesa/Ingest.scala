package com.azavea.ingest.geomesa

import com.typesafe.scalalogging.Logger
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.feature.simple._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.referencing.operation.transform.AffineTransform2D
import org.opengis.feature.simple._
import org.opengis.feature.`type`.Name

import java.io.File
import java.util.HashMap
import scala.collection.JavaConversions._

import com.azavea.ingest.common._

object Ingest {
  trait CSVorSHP
  case object CSV extends CSVorSHP
  case object SHP extends CSVorSHP
  implicit val readsCSVorSHP = scopt.Read.reads[CSVorSHP]({ s: String =>
    s.toLowerCase match {
      case "csv" => CSV
      case "shp" => SHP
      case "shapefile" => SHP
      case _ => throw new IllegalArgumentException("Must choose either CSV or SHP")
    }
  })


  case class Params (csvOrShp: CSVorSHP = CSV,
                     instanceId: String = "geomesa",
                     zookeepers: String = "zookeeper",
                     user: String = "root",
                     password: String = "secret",
                     tableName: String = "",
                     dropLines: Int = 0,
                     separator: String = "\t",
                     codec: CSVSchemaParser.Expr = CSVSchemaParser.Spec(Nil),
                     featureName: String = "default-feature-name",
                     s3bucket: String = "",
                     s3prefix: String = "",
                     csvExtension: String = ".csv",
                     translate: Boolean = false, 
                     origin: Array[Double] = Array(0.0, 0.0),
                     numReplicates: Int = 0,
                     centerFile: String = "",
                     unifySFT: Boolean = true) {

    def convertToJMap(): HashMap[String, String] = {
      val result = new HashMap[String, String]
      result.put("instanceId", instanceId)
      result.put("zookeepers", zookeepers)
      result.put("user", user)
      result.put("password", password)
      result.put("tableName", tableName)
      println(result)
      result
    }
  }


  //def registerSFTs(cli: Params, sft: SimpleFeatureType) = {
  //  val ds = DataStoreFinder.getDataStore(cli.convertToJMap)
  //  if (ds == null) {
  //    println("Could not build AccumuloDataStore")
  //    java.lang.System.exit(-1)
  //  }
  //  ds.createSchema(sft)
  //  ds.dispose
  //}

  def registerSFT(cli: Params)(sft: SimpleFeatureType) = {
    val ds = DataStoreFinder.getDataStore(cli.convertToJMap)

    if (ds == null) {
      println("Could not build AccumuloDataStore")
      java.lang.System.exit(-1)
    }

    ds.createSchema(sft)
    ds.dispose
  }

  def ingestRDD(cli: Params)(rdd: RDD[SimpleFeature]) =
    /* The method for ingest here is based on:
     * https://github.com/locationtech/geomesa/blob/master/geomesa-tools/src/main/scala/org/locationtech/geomesa/tools/accumulo/ingest/AbstractIngest.scala#L104
     */

    rdd.foreachPartition({ featureIter =>
      val ds = DataStoreFinder.getDataStore(cli.convertToJMap)

      if (ds == null) {
        println("Could not build AccumuloDataStore")
        java.lang.System.exit(-1)
      }

      var fw: FeatureWriter[SimpleFeatureType, SimpleFeature] = null
      try {
        fw = ds.getFeatureWriterAppend(cli.featureName, Transaction.AUTO_COMMIT)
        featureIter.toStream.foreach({ feature: SimpleFeature =>
            val toWrite = fw.next()
            toWrite.setAttributes(feature.getAttributes)
            toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
            toWrite.getUserData.putAll(feature.getUserData)
            toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          try {
            fw.write()
          } catch {
            case e: Exception => throw e //println(s"Failed to write a feature", e)
          }
        })
      } finally {
        fw.close()
        ds.dispose()
      }
    })

  def shiftAndIngest(params: Params)(tuples: RDD[((Double, Double), SimpleFeature)]) = {
    val oldCenter = params.origin
    val exemplar = tuples.first._2

    val origSFT = exemplar.getType
    val origCRS = origSFT.getCoordinateReferenceSystem
    val newCRS = CRS.decode("EPSG:4326")
    val coordChange = CRS.findMathTransform(origCRS, newCRS, true)

    val sftBuilder = new SimpleFeatureTypeBuilder
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(origSFT.getAttributeDescriptors)
    origSFT.getUserData.map{case (k,v) => sftBuilder.userData(k,v)}
    sftBuilder.setCRS(newCRS)
    val newSFT = sftBuilder.buildFeatureType

    val newRDD = tuples.mapPartitions(iter => iter.map { case (newCenter, feature) => {
      val shift = new java.awt.geom.AffineTransform
      shift.translate(newCenter._1 - oldCenter(0), newCenter._2 - oldCenter(1))

      val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
      val newGeom = JTS.transform(JTS.transform(geom, coordChange), new AffineTransform2D(shift))

      val builder = new SimpleFeatureBuilder(newSFT)
      newSFT.getAttributeDescriptors.foreach { attr => 
        if (classOf[Geometry].isAssignableFrom(attr.getType.getBinding))
          builder.add(newGeom)
        else
          builder.add(feature.getAttribute(attr.getName.toString))
      }
      builder.buildFeature(feature.getID)
      }
    }, true)

    ingestRDD(params)(newRDD)
  }

}
