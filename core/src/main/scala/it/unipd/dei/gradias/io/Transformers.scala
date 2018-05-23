package it.unipd.dei.gradias.io

import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unipd.dei.gradias.util.Statistics.{Skewed, Uniform, Distribution, Exponential}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

trait Transformer {

  def requirements: Map[String, Any] = Map()

  def meta: Map[String, Any] = Map()

  def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)]

  def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)]

}

/**
 * Functions used to transform a stream of edges
 */
object Transformers {

  def fromString(desc: String): Transformer = desc match {
    case "remap" => new IdRemapTransformer()
    case "canonical" => new CanonicalOrientationFilter()
    case "symmetric" => new SymmetrizeTransformer()
    case "uniq" => new UniqTransformer()
    case "ceil" => new WeightCeilTransformer()
    case "rescale" => new RescaleTransformer()
    case "" => new Transformer {
      override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = edges
      override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = edges
    }
    case reweight if reweight.startsWith("reweight") =>
      new ReweightTransformer(reweight.substring("reweight".length))
    case err => throw new IllegalArgumentException("Unsupported transformer " + err)
  }

  def fromRecipe(recipe: String): Transformer = {
    val ts = recipe.split("\\|").map(d => fromString(d.trim))
    chain(ts)
  }

  def checkCompatible(a: Transformer, b: Transformer): Boolean = {
    for ((k, v) <- b.requirements) {
      if(!v.equals(a.meta(k))) {
        return false
      }
    }
    true
  }

  def chain(a: Transformer, b: Transformer): Transformer = {
    require(checkCompatible(a, b), "Incompatible transformers")
    new Transformer {
      override def requirements: Map[String, Any] = a.requirements

      override def meta: Map[String, Any] =
        a.meta ++ b.meta

      override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] =
        b.apply(a.apply(edges))

      override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] =
        b.parallel(a.parallel(edges))
    }
  }

  def chain(ts: Seq[Transformer]): Transformer = {
    ts.reduceLeft(chain)
  }

  class WeightCeilTransformer extends Transformer with Serializable {
    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      println("Taking the ceil of edge weights")
      edges.map { case (eKey, weight) =>
        (eKey, math.ceil(weight).toFloat)
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      println("Taking the ceil of edge weights")
      edges.map { case (eKey, weight) =>
        (eKey, math.ceil(weight).toFloat)
      }
    }
  }

  class RescaleTransformer() extends Transformer with Serializable {

    override def meta: Map[String, Any] =
      Map("edges.rescaled" -> true)

    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = ???

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      val maxWeight = edges.values.reduce(math.max)
      edges.mapValues(_ / maxWeight)
    }

  }

  class ReweightTransformer(val distributionDesc: String) extends Transformer with Serializable {

    val distribution: Distribution = getDistribution(distributionDesc)

    override def requirements: Map[String, Any] =
      Map("edges.canonical-orientation" -> true)

    override def meta: Map[String, Any] = Map(
      "edges.reweigted" -> true,
      "edges.weight.distribution" -> distributionDesc
    )

    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      println("Reweighting with " + distributionDesc)
      edges.map { case (eKey, weight) =>
        (eKey, math.max(distribution.sample.toFloat, Float.MinPositiveValue))
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      algoLogger.info("Reweighting with " + distributionDesc)
      edges.mapPartitions({ it =>
        val dist = getDistribution(distributionDesc)
        it.map { case (eKey, weight) =>
          (eKey, math.max(dist.sample.toFloat, Float.MinPositiveValue))
        }
      }, preservesPartitioning = true)

    }

    def getDistribution(descr: String): Distribution = {
      val distribution = """(\w+)\((.*?)\)""".r
      descr.trim match {
        case distribution(name, params) =>
          name match {
            case "uniform" =>
              params.split(",").toList match {
                case List("") => new Uniform(0.0, 1.0)
                case List(low, high) => new Uniform(low.toDouble, high.toDouble)
                case List(high) => new Uniform(0.0, high.toDouble)
                case _ => throw new IllegalArgumentException(s"Wrong parameter list $params")
              }
            case "exponential" =>
              val beta = params.toDouble
              new Exponential(1/beta)
            case "skewed" =>
              params.split(",").toList match {
                case List("") => new Skewed()
                case List(heavyProb, lowVal) => new Skewed(heavyProb.toDouble, lowVal.toDouble)
                case List(heavyProb) => new Skewed(heavyProb.toDouble)
                case _ => throw new IllegalArgumentException(s"Wrong parameter list $params")
              }
            case _ => throw new IllegalArgumentException(s"Unknown distribution `$name`")
          }
        case _ => throw new IllegalArgumentException(descr)
      }
    }

  }

  class CanonicalOrientationFilter extends Transformer {

    override def meta: Map[String, Any] = Map(
      "edges.canonical-orientation" -> true,
      "edges.symmetric" -> false
    )

    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      println("Filtering with canonical order")
      edges.filter { case (eKey, _) => eKey.src < eKey.dst }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      algoLogger.info("Filtering with canonical order")
      edges.filter { case (eKey, _) => eKey.src < eKey.dst }
    }

  }

  /**
   * Remap the Ids so that they fall in the range `[0..n]`
   */
  class IdRemapTransformer extends Transformer {

    override def meta: Map[String, Any] = Map(
      "graph.remapped" -> true,
      "edges.canonical-orientation" -> false)

    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      println("Remapping IDs")
      val m = new Long2LongOpenHashMap()
      var cnt = 0L

      edges.map { case (eKey, weight) =>
        if(!m.containsKey(eKey.src)) {
          m.put(eKey.src, cnt)
          cnt += 1
        }
        if(!m.containsKey(eKey.dst)) {
          m.put(eKey.dst, cnt)
          cnt += 1
        }
        (new EdgeKey(m.get(eKey.src), m.get(eKey.dst)), weight)
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = ???
  }

  class SymmetrizeTransformer extends Transformer {

    override def meta: Map[String, Any] = Map(
      "edges.canonical-orientation" -> false,
      "edges.symmetric" -> true
    )

    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      println("Symmetrizing edges")
      edges.flatMap { case (eKey, w) =>
        Seq((new EdgeKey(eKey.src, eKey.dst), w), (new EdgeKey(eKey.dst, eKey.src), w))
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      algoLogger.info("Symmetrizing edges")
      edges.flatMap { case (eKey, w) =>
        Seq((new EdgeKey(eKey.src, eKey.dst), w), (new EdgeKey(eKey.dst, eKey.src), w))
      }
    }
  }

  class UniqTransformer extends Transformer {
    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      val last = new EdgeKey()
      edges.flatMap { case pair@(eKey, v) =>
        if(eKey.equals(last)) {
          Seq.empty
        } else {
          last.src = eKey.src
          last.dst = eKey.dst
          Seq(pair)
        }
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = {
      edges.reduceByKey((a, b) => a)
    }
  }

  class Echoer extends Transformer {
    override def apply(edges: Iterator[(EdgeKey, Float)]): Iterator[(EdgeKey, Float)] = {
      edges.map { pair =>
        println(s"echo: $pair")
        pair
      }
    }

    override def parallel(edges: RDD[(EdgeKey, Float)]): RDD[(EdgeKey, Float)] = ???
  }

}
