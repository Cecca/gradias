/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.gradias

import java.util.{Date, Calendar}
import java.util.jar.Attributes.Name._

import it.unipd.dei.gradias.anf.HadiDriver
import it.unipd.dei.gradias.bfs.BfsDriver
import it.unipd.dei.gradias.converter.{MatToAdjConverter, TextInputConverter}
import it.unipd.dei.gradias.decomposition.cluster.ClusterDriver
import it.unipd.dei.gradias.decomposition.delayed.Mpx13Driver
import it.unipd.dei.gradias.diameter.WeightedDiameter
import it.unipd.dei.gradias.diameter.WeightedDiameter.Algorithm
import it.unipd.dei.gradias.lifecycle.{DeltaSSSPLifecycle, DistributedSSSPLifeclycle, DeltaClusterLifecycle, GraphxClusterLifecycle}
import it.unipd.dei.gradias.serialization.KryoSerialization
import it.unipd.dei.gradias.util.Logger.toolLogger
import it.unipd.dei.gradias.util.ManifestInfo.manifestMap
import it.unipd.dei.gradias.util.{Utils, Logger, ManifestKeys}
import it.unipd.dei.gradias.weighted.decomposition.RefinementStrategy
import it.unipd.dei.gradias.weighted.sssp._
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ArgType, ScallopConf, Subcommand, ValueConverter}

import scala.language.{implicitConversions, reflectiveCalls}
import scala.reflect.runtime.universe._


/**
 * Main entry point for the entire application
 */
object Tool extends TextInputConverter
with KryoSerialization
with MatToAdjConverter {

  lazy val sparkContext = {
    val conf = new SparkConf(loadDefaults = true)
      .setAppName("Spark Graph Algorithms")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", registratorName)
      // TODO set to false
      .set("spark.shuffle.consolidateFiles", "true")

    GraphXUtils.registerKryoClasses(conf)

    val sparkContext = new SparkContext(conf)
    Logger.algoLogger.info(s"Spark context started at ${new Date(sparkContext.startTime)}")

    sparkContext.setCheckpointDir(conf.get("spark.checkpoint.dir", "/tmp/spark-checkpoint"))

    sparkContext
  }

  def main(args: Array[String]) {

    manifestMap.foreach { m =>
      toolLogger.info(
        s"""
          |=== Spark Graph ========================================================
          |
          |    Version  : ${m.getOrElse(IMPLEMENTATION_VERSION, "-")}
          |    Branch   : ${m.getOrElse(ManifestKeys.GIT_BRANCH, "-")}
          |    Built on : ${m.getOrElse(ManifestKeys.GIT_BUILD_DATE, "-")}
          |    Git rev  : ${m.getOrElse(ManifestKeys.GIT_HEAD_REV, "-")}
          |
          |========================================================================""".stripMargin)
    }

    val conf = new CmdLineConf(args)

    conf.subcommands match {

      case List(cmd @ conf.cluster) =>
        val clusterAlg = new ClusterDriver(
          sparkContext,
          cmd.input(),
          cmd.targetFraction(),
          cmd.skipDiameter(),
          cmd.diameterAlgorithm())
        clusterAlg()

      // Delayed version ----------------------------------------------------
      case List(cmd @ conf.mpx13) =>
        val mpx13Alg = new Mpx13Driver(
          sparkContext,
          cmd.input(),
          cmd.beta(),
          cmd.targetFraction(),
          cmd.skipDiameter(),
          cmd.diameterAlgorithm())
        mpx13Alg()

      // HADI ----------------------------------------------------------------
      case List(cmd @ conf.hadi) =>
        val algorithm = new HadiDriver(sparkContext, cmd.input(), cmd.numCounters())
        algorithm()

      // BFS -----------------------------------------------------------------
      case List(cmd @ conf.bfs) =>
        val algorithm = new BfsDriver(sparkContext, cmd.input())
        algorithm()

      // SSSP -------------------------------------------------------------
      case List(cmd @ conf.sssp) =>
        new DistributedSSSPLifeclycle(sparkContext, cmd.input(), cmd.implementation()).execute()

      // Delta cluster -------------------------------------------------------------
      case List(cmd @ conf.deltaCluster) =>
        val life = new DeltaClusterLifecycle(
          sparkContext,
          cmd.input(),
          cmd.target(),
          Utils.getDelta(cmd.delta(), cmd.input(), sparkContext),
          RefinementStrategy.get(cmd.refinement()),
          cmd.steps(),
          cmd.implementation(),
          cmd.diameterAlgorithm(),
          cmd.heuristic.get)
        life.execute()

      case List(cmd @ conf.deltaClusterGraphx) =>
        val life = new GraphxClusterLifecycle(
          sparkContext,
          cmd.input(),
          cmd.target(),
          cmd.delta())
        life.execute()


      // Delta stepping -------------------------------------------------------------
      case List(cmd @ conf.deltaSssp) =>
        val life = new DeltaSSSPLifecycle(
          sparkContext,
          cmd.input(),
          cmd.source.get,
          cmd.delta(),
          cmd.implementation())
        life.execute()

      // Default help printing ------------------------------------------------
      case Nil => conf.printHelp()

      case subs =>
        System.err.println("Unknown subcommand:\n")
        System.err.println(
          subs.map(_.builder.getSubcommandName).mkString("\n\t"))
        conf.printHelp()
    }

  }

  // -------------------------------------------------------------------------
  //    Command line
  // -------------------------------------------------------------------------

  trait VersionBanner extends ScallopConf {
    val progName = {
      for(manifest <- manifestMap;
          n <- manifest.get(IMPLEMENTATION_TITLE)) yield n
    }.getOrElse("gradias")

    val progVer = {
      for(manifest <- manifestMap;
          v <- manifest.get(IMPLEMENTATION_VERSION)) yield v
    }.getOrElse("-")

    version(s"$progName version $progVer (c) 2013-2014 Matteo Ceccarello")
  }

  class CmdLineConf(args: Seq[String]) extends ScallopConf(args) with VersionBanner {

    shortSubcommandsHelp(true)

    banner(
      s"""
        |Usage: spark-submit [spark-opts] --class it.unipd.dei.gradias.Tool $progName-$progVer.jar <subcommand> [options]
        |
        |Options:
      """.stripMargin)

    val cluster = new Subcommand("cluster") with IOOptions with DiameterOptions with VersionBanner {
      val targetFraction = opt[Double](required = true)
    }

    val mpx13 = new Subcommand("mpx13") with IOOptions with DiameterOptions with VersionBanner {
      val beta = opt[Double](required = true,
        descr="the inverse of the mean of the exponential distribution")
      val targetFraction = opt[Double](default=Some(0.0))
    }

    val hadi = new Subcommand("hadi") with IOOptions with VersionBanner {
      val numCounters = opt[Int](default = Some(32),
        descr="the number of counters to use")
    }

    val bfs = new Subcommand("bfs") with IOOptions with VersionBanner {
      banner("Baseline: Computes the diameter with BFS")
    }
    
    val sssp = new Subcommand("sssp") with IOOptions with VersionBanner {
      banner("Baseline: Computes the diameter with Single Source Shortest Path")
      val symmetric = toggle(default = Some(true))
      val implementation = opt[String](default = Some("adjacency"))
    }

    val deltaSssp = new Subcommand("delta-sssp") with IOOptions with VersionBanner {
      banner("Baseline: Computes the diameter with Delta Stepping")
      val delta = opt[Distance](required = true)
      val source = opt[NodeId](required=false)
      val implementation = opt[String](default = Some("adjacency"))
    }

    val deltaCluster = new Subcommand("delta-cluster") with IOOptions with DiameterOptions with VersionBanner {
      val target = opt[Long](required = true)
      val delta = opt[String](required = true)
      val steps = opt[Int](default = Some(1))
      val refinement = opt[String](default = Some("stop"))
      val implementation = opt[String](default = Some("adjacency"))
      val heuristic = opt[String](default = None)
    }

    val deltaClusterGraphx = new Subcommand("delta-cluster-graphx") with IOOptions with DiameterOptions with VersionBanner {
      val target = opt[Int](required = true)
      val delta = opt[Distance](required = true)
    }

  }

  trait IOOptions extends ScallopConf {
    val input = opt[String](required = true,
      descr="the input graph")
    val output = opt[String](
      descr="the output file. If not given, no output is written")
  }

  trait DiameterOptions extends ScallopConf with OneArgConverter {

    val algOptConverter = oneArgConverter {
      case s if WeightedDiameter.algorithms.contains(s) => s
      case notFound =>
        throw new IllegalArgumentException(
          s"""
            |Diameter algorithm $notFound not found.
            |
            |Available algorithms are
            |
            |  ${WeightedDiameter.algorithms.mkString(", ")}
          """.stripMargin)
    }

    val skipDiameter = toggle(default = Some(false))
    val diameterAlgorithm = opt[Algorithm](default = Some("Distributed"))(conv = algOptConverter)
  }

  trait OneArgConverter {

    def oneArgConverter[A](conv: String => A)(implicit tt: TypeTag[A]) = new ValueConverter[A] {
      def parse(s: List[(String, List[String])]) = {
        s match {
          case (_, i :: Nil) :: Nil =>
            try { Right(Some(conv(i))) } catch { case e: Throwable =>
              Left(s"wrong arguments format: ${e.getMessage}") }
          case Nil => Right(None)
          case _ => Left("you should provide exactly one argument for this option")
        }
      }
      val tag = tt
      val argType = ArgType.SINGLE
    }

  }

}
