package com.azavea.ingest.geowave

import mil.nga.giat.geowave.adapter.vector._
import mil.nga.giat.geowave.core.geotime.ingest._
import mil.nga.giat.geowave.core.store.index._
import mil.nga.giat.geowave.datastore.accumulo._
import mil.nga.giat.geowave.datastore.accumulo.index.secondary._
import mil.nga.giat.geowave.datastore.accumulo.metadata._
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions
import mil.nga.giat.geowave.core.geotime.index.dimension._
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.{ Unit => BinUnit }
import mil.nga.giat.geowave.core.geotime.ingest._
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory
import mil.nga.giat.geowave.core.store.DataStore
import mil.nga.giat.geowave.core.store.index.PrimaryIndex
import mil.nga.giat.geowave.core.store.index.writer.IndexWriter
import mil.nga.giat.geowave.datastore.accumulo._
import org.geotools.data.{DataStoreFinder, FeatureSource}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.FeatureCollection
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.opengis.filter.Filter
import org.apache.spark.rdd._

import java.util.HashMap
import scala.collection.JavaConversions._
import scala.util.Try

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
                     instanceId: String = "geowave",
                     zookeepers: String = "zookeeper",
                     user: String = "root",
                     password: String = "GisPwd",
                     tableName: String = "",
                     dropLines: Int = 0,
                     separator: String = "\t",
                     codec: CSVSchemaParser.Expr = CSVSchemaParser.Spec(Nil),
                     featureName: String = "default-feature-name",
                     s3bucket: String = "",
                     s3prefix: String = "",
                     csvExtension: String = ".csv",
                     temporal: Boolean = false,
                     unifySFT: Boolean = true)

  def registerSFT(params: Params)(sft: SimpleFeatureType): Unit = ???

  def getGeowaveDataStore(conf: Params): AccumuloDataStore = {
    // GeoWave persists both the index and data adapter to the same accumulo
    // namespace as the data. The intent here
    // is that all data is discoverable without configuration/classes stored
    // outside of the accumulo instance.
    val instance = new BasicAccumuloOperations(conf.zookeepers,
                                               conf.instanceId,
                                               conf.user,
                                               conf.password,
                                               conf.tableName)

    val options = new AccumuloOptions
    options.setPersistDataStatistics(true)
    //options.setUseAltIndex(true)

    return new AccumuloDataStore(
      new AccumuloIndexStore(instance),
      new AccumuloAdapterStore(instance),
      new AccumuloDataStatisticsStore(instance),
      new AccumuloSecondaryIndexDataStore(instance),
      new AccumuloAdapterIndexMappingStore(instance),
      instance,
      options)
  }

  def ingestRDD(params: Params)(rdd: RDD[SimpleFeature]) =
    rdd.foreachPartition({ featureIter =>
      val features = featureIter.buffered
      val ds = getGeowaveDataStore(params)

      val adapter = new FeatureDataAdapter(features.head.getType())
      val index =
        if (params.temporal) {
          (new SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder).createIndex
        } else {
          (new SpatialDimensionalityTypeProvider.SpatialIndexBuilder).createIndex
        }
      val indexWriter = ds.createWriter(adapter, index).asInstanceOf[IndexWriter[SimpleFeature]]
      try {
        features.foreach({ feature => indexWriter.write(feature) })
      } finally {
        indexWriter.close()
      }
    })
}

