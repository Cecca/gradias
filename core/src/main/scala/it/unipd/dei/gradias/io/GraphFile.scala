package it.unipd.dei.gradias.io

import it.unipd.dei.gradias.weighted.decomposition.DeltaCluster.Vertex
import it.unipd.dei.gradias.{BlockGraph, generators}
import it.unipd.dei.gradias.io.Transformers._
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.util.{ProfilingListener, ProgressLoggerListener}
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{Reader, Sorter}
import org.apache.hadoop.io.WritableComparable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.language.reflectiveCalls
import scala.util.Random

object GraphFile {

  def main(args: Array[String]) {
    val opts = new Options(args)

    opts.subcommands match {
      // Show info on the graph
      case List(cmd@opts.show) =>
        val meta = Inputs.metadata(cmd.graph())
        println(s"=== Metadata for ${cmd.graph()} ===\n")
        println(meta.toMap.mkString("\n"))
        Inputs.sequenceKeyValueClass(cmd.graph()).foreach { case (kClass, vClass) =>
          println()
          println("Key class:   " + kClass)
          println("Value class: " + vClass)
        }
        if(cmd.stats()) {
          println("=== Statistics ===")
          val stats = StatsCollector.collect(cmd.graph())
          println(stats.mkString("\n"))
        }

        if (cmd.take.isDefined) {
          val toTake = cmd.take()
          println(s"\n=== First $toTake edges ===\n")
          val edges = Inputs.sequenceFile(cmd.graph()).take(toTake)
          println(edges.mkString("\n"))
        }
        if (cmd.grepSrc.isDefined) {
          println(s"\n=== Edges with source ${cmd.grepSrc()} ===\n")
          val edges = Inputs.sequenceFile(cmd.graph()).filter(_._1.src == cmd.grepSrc())
          println(edges.mkString("\n"))
        }
        if (cmd.grepDst.isDefined) {
          println(s"\n=== Edges with destination ${cmd.grepDst()} ===\n")
          val edges = Inputs.sequenceFile(cmd.graph()).filter(_._1.dst == cmd.grepDst())
          println(edges.mkString("\n"))
        }

      // generate graphs
      case List(cmd@opts.generate) =>
        val generator = generators.getGenerator(cmd.generator())
        val graph: Iterator[(EdgeKey, Float)] =
          if(cmd.remap())
            new IdRemapTransformer().apply(generator.generate())
          else
            generator.generate()
        Outputs.writeSequenceFile(cmd.output(), graph)
        val metadata = Metadata.forFile(cmd.output())
        metadata.setAll(
          Map(
            "graph.generator" -> cmd.generator(),
            "graph.remapped" -> cmd.remap()))
        metadata.save()
        algoLogger.info("Done!")

      case List(cmd@opts.transform) =>

        val metaIn = Inputs.metadata(cmd.input())
        val metaOut = Metadata.forFile(cmd.output())

        val inputGraph = Inputs.readInput(cmd.input())
        val transformer = Transformers.fromRecipe(cmd.recipe())
        val transformed = transformer(inputGraph)
        metaIn.setAll(transformer.meta)
        Outputs.writeSequenceFile(cmd.output(), transformed)
        metaOut.setAll(metaIn.toMap.filterKeys(k => !StatsCollector.statsKeys.contains(k)))
        metaOut.setAll(transformer.meta)
        metaOut.save()
        algoLogger.info("Done!")

      case List(cmd@opts.parGenerate) =>
        val meta = Metadata.forFile(cmd.output())
        val conf = new SparkConf(loadDefaults = true)
        conf
          .setAppName("Dataset transformations")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "it.unipd.dei.gradias.serialization.GraphKryoRegistrator")
        val sc = new SparkContext(conf)
        sc.addSparkListener(new ProgressLoggerListener())
        sc.addSparkListener(new ProfilingListener())

        val generator = generators.getGenerator(cmd.generator())
        val generated = generator.parallel(sc)
        generated.saveAsSequenceFile(cmd.output())
        meta.setAll(Map("graph.generator" -> cmd.generator()))
        meta.save()
        algoLogger.info("Done!")

      case List(cmd@opts.parTransform) =>
        val metaIn = Inputs.metadata(cmd.input())
        val metaOut = Metadata.forFile(cmd.output())
        val conf = new SparkConf(loadDefaults = true)
          .setAppName("Dataset transformations")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryo.registrator", "it.unipd.dei.gradias.serialization.GraphKryoRegistrator")
        val sc = new SparkContext(conf)
        sc.addSparkListener(new ProgressLoggerListener())
        sc.addSparkListener(new ProfilingListener())
        val inputGraph: RDD[(EdgeKey, Float)] = sc.sequenceFile(cmd.input())
        val transformer = Transformers.fromRecipe(cmd.recipe())
        val transformed = transformer.parallel(inputGraph)
        transformed.saveAsSequenceFile(cmd.output())
        metaOut.setAll(metaIn.toMap.filterKeys(k => !StatsCollector.statsKeys.contains(k)))
        metaOut.setAll(transformer.meta)
        metaOut.save()
        algoLogger.info("Done!")

      case List(cmd@opts.updateMeta) =>
        // Load (eventual) embedded metadata and saves it in the new format
        val meta = Inputs.metadata(cmd.file())
        meta.save()

    }
  }

  class Options(args: Array[String]) extends ScallopConf(args) {

    val transform = new Subcommand("transform") {
      val input = opt[String](required = true)
      val output = opt[String](required = true)
      val recipe = opt[String](default=Some(""))
    }

    val parTransform = new Subcommand("parallel-transform") {
      val input = opt[String](required = true)
      val output = opt[String](required = true)
      val recipe = opt[String](default=Some(""))
    }

    val show = new Subcommand("show") {
      val graph = trailArg[String](name="GRAPH", required = true)
      val take = opt[Int]()
      val stats = toggle(default=Some(false))
      val grepSrc = opt[Long]()
      val grepDst = opt[Long]()
    }

    val generate = new Subcommand("generate") {
      val output = opt[String](required = true)
      val remap = toggle(default = Some(false))
      val generator = trailArg[String](name = "GENERATOR", required = true)
    }

    val parGenerate = new Subcommand("parallel-generate") {
      val output = opt[String](required = true)
      val generator = trailArg[String](name = "GENERATOR", required = true)
    }

    val updateMeta = new Subcommand("update-meta") {
      val file = trailArg[String](name="GRAPH", required = true)
    }
  }

}
