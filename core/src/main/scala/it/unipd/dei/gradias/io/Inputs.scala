package it.unipd.dei.gradias.io

import java.io.{File, IOException}
import java.util.Properties

import com.esotericsoftware.kryo.io.Input
import it.unimi.dsi.logging.ProgressLogger
import it.unimi.dsi.webgraph.{BVGraph, Transform}
import it.unipd.dei.gradias.{AdjacencyBlock, NodeAttributesBlock, BlockGraph}
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.weighted.decomposition.DeltaCluster.Vertex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.{BytesWritable, FloatWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.collection.JavaConversions
import scala.io.Source
import scala.language.reflectiveCalls
import scala.reflect.ClassTag

object Inputs {

  def readInput(file: String): Iterator[(EdgeKey, Float)] = {
    if(file.endsWith(".adj")) {
      algoLogger.info(s"Reading $file as an adjacency file")
      adjacencyFile(file)
    } else if (file.endsWith(".edges") || file.endsWith(".wedges")) {
      algoLogger.info(s"Reading $file as an edge list file")
      edgesFile(file)
    } else if (new File(file + ".properties").isFile) {
      algoLogger.info(s"Reading $file as a webgraph file")
      webGraph(file)
    } else {
      val reader = new Reader(new Configuration(), Reader.file(new Path(file)))
      val keyClass = reader.getKeyClass
      if(keyClass.equals(classOf[EdgeKey])) {
        algoLogger.info(s"Reading $file as a sequence file")
        sequenceFile(file)
      } else if (keyClass.equals(classOf[EdgeLongWeight])) {
        algoLogger.info(s"Reading $file as a legacy sequence file")
        sequenceFileLegacy(file)
      } else {
        throw new IllegalArgumentException(s"Unsupported key class $keyClass")
      }
    }
  }

  def adjacencyFile(file: String): Iterator[(EdgeKey, Float)] =
    Source.fromFile(file).getLines().flatMap{ line =>
      if(line.startsWith("#")) {
        Seq.empty
      } else {
        val elems = line.split("\\s+")
        if(elems.length >= 2) {
          val src = elems.head.toLong
          for (e <- elems.tail) yield (new EdgeKey(src, e.toLong), 1.0f)
        } else {
          Seq.empty
        }
      }
    }

  def edgesFile(file: String): Iterator[(EdgeKey, Float)] =
    Source.fromFile(file).getLines().filterNot(_.startsWith("#")).map { line =>
      val elems = line.split("\\s+")
      if(elems.length == 2) (new EdgeKey(elems(0).toLong, elems(1).toLong), 1.0f)
      else if(elems.length == 3) (new EdgeKey(elems(0).toLong, elems(1).toLong), elems(2).toFloat)
      else throw new IllegalArgumentException(
        s"Each line should have 2 or 3 items. The current line is `$line`")
    }

  def sequenceFile(file: String): Iterator[(EdgeKey, Float)] = {
    val path = new Path(file)
    val conf = new Configuration()
    if (path.getFileSystem(conf).isFile(path)) {
      algoLogger.info("Reading a single sequence file")
      singleSequenceFile(path, conf)
    } else {
      algoLogger.info("Reading multiple sequence files")
      multiSequenceFile(path, conf)
    }
  }

  private def singleSequenceFile(path: Path, conf: Configuration): Iterator[(EdgeKey, Float)] = {
    algoLogger.info(s"Read $path")
    val reader = new Reader(conf, Reader.file(path))
    require(reader.getKeyClass.equals(classOf[EdgeKey]),
      s"Key class should be ${classOf[EdgeKey]}")
    require(reader.getValueClass.equals(classOf[FloatWritable]),
      s"Value class should be ${classOf[FloatWritable]}")

    new Iterator[(EdgeKey, Float)] {
      val edgeKey = new EdgeKey()
      val nextEdgeKey = new EdgeKey()
      val edgeWeight = new FloatWritable()
      val nextEdgeWeight = new FloatWritable()
      var _hasNext = true

      // initialize the iterator
      _hasNext = reader.next(nextEdgeKey, nextEdgeWeight)

      override def hasNext: Boolean = _hasNext

      override def next(): (EdgeKey, Float) = {
        if(!_hasNext)
          throw new NoSuchElementException("No next element")
        edgeKey.src = nextEdgeKey.src
        edgeKey.dst = nextEdgeKey.dst
        edgeWeight.set(nextEdgeWeight.get)

        _hasNext = reader.next(nextEdgeKey, nextEdgeWeight)

        (edgeKey, edgeWeight.get())
      }
    }
  }

  private def multiSequenceFile(path: Path, conf: Configuration): Iterator[(EdgeKey, Float)] = {
    val fs = path.getFileSystem(conf)
    require(fs.isDirectory(path))
    fs.listStatus(path).iterator.filter(fs => fs.getPath.getName.startsWith("part-")).flatMap { fileStatus =>
      val file = fileStatus.getPath
      singleSequenceFile(file, conf)
    }
  }

  def sequenceFileLegacy(file: String): Iterator[(EdgeKey, Float)] = {
    val conf = new Configuration()
    val reader = new Reader(conf, Reader.file(new Path(file)))
    require(reader.getKeyClass.equals(classOf[EdgeLongWeight]),
      s"Key class should be ${classOf[EdgeLongWeight]}")
    require(reader.getValueClass.equals(classOf[NullWritable]),
      s"Value class should be ${classOf[NullWritable]}")

    new Iterator[(EdgeKey, Float)] {
      val edge = new EdgeLongWeight()
      val nextEdge = new EdgeLongWeight()
      var _hasNext = true

      // initialize iterator
      _hasNext = reader.next(edge)

      override def hasNext: Boolean = _hasNext

      override def next(): (EdgeKey, Float) = {
        edge.src = nextEdge.src
        edge.dst = nextEdge.dst
        edge.weight = nextEdge.weight
        _hasNext = reader.next(edge)
        (new EdgeKey(edge.src, edge.dst), edge.weight.toFloat)
      }
    }
  }

  @deprecated("Use the new SequenceFile", "0.11.0")
  def sequenceFileRaw(file: String): Iterator[EdgeLongWeight] = {
    val conf = new Configuration()
    val reader = new Reader(conf, Reader.file(new Path(file)))

    new Iterator[EdgeLongWeight] {
      val edge = new EdgeLongWeight(-1, -1, -1)
      var _hasNext = true

      override def hasNext: Boolean = _hasNext

      override def next(): EdgeLongWeight = {
        _hasNext = reader.next(edge)
        edge
      }
    }
  }

  def metadata(file: String): Metadata = {
    val meta = Metadata.forFile(file)
    sequenceFileMetadata(file) foreach { m =>
      algoLogger.info("Loading embedded metadata")
      meta.setAll(m)
    }

    meta
  }

  @deprecated("Use Inputs.metadata", "0.11.0")
  def sequenceFileMetadata(file: String): Option[Map[String, String]] = {
    val conf = new Configuration()
    try {
      val reader = new Reader(conf, Reader.file(new Path(file)))

      val meta = JavaConversions.mapAsScalaMap(reader.getMetadata.getMetadata).map {
        case (k, v) => (k.toString, v.toString)
      }.toMap
      Some(meta)
    } catch {
      case e: IOException =>
        algoLogger.debug("Unable to retrieve embedded metadata: {}", e.getMessage)
        None
    }
  }

  def sequenceKeyValueClass(file: String): Option[(Class[_], Class[_])] = {
    val conf = new Configuration()
    try {
      val reader = new Reader(conf, Reader.file(new Path(file)))
      Some((reader.getKeyClass, reader.getValueClass))
    } catch {
      case e: IOException =>
        algoLogger.warn("Unable to retrieve key and value classes: {}", e.getMessage)
        None
    }
  }

  def webGraph(file: String): Iterator[(EdgeKey, Float)] = {
    val pl = new ProgressLogger(algoLogger, "nodes")
    pl.displayFreeMemory = true

    val graphProps = new Properties()
    graphProps.load(Source.fromFile(new File(file + ".properties")).reader())
    val gClass = graphProps.getProperty("graphclass")
    val numNodes = graphProps.getProperty("nodes").toLong
    val numEdges = graphProps.getProperty("arcs").toLong
    algoLogger.info(s"Reading $gClass with $numNodes nodes and $numEdges edges")

    val raw = BVGraph.loadOffline(file, pl)
    val transposedFileName = file + "-t"
    val transposed =
      if (new File(transposedFileName + ".properties").isFile) {
        algoLogger.info(s"Found transposed graph $transposedFileName, using it for symmetrization")
        BVGraph.loadOffline(transposedFileName, pl)
      } else {
        null
      }
    val g = Transform.symmetrize(raw, transposed, pl)
    val nodeIt = g.nodeIterator()

    pl.start()
    pl.expectedUpdates = numNodes

    new Iterator[(EdgeKey, Float)] {

      var curNode = 0
      var curSuccessorArray: Array[Int] = null
      var curOutDegree = 0
      var curSuccessorIdx = 0

      override def hasNext: Boolean =
        curSuccessorIdx < curOutDegree || nodeIt.hasNext

      override def next(): (EdgeKey, Float) = {
        if (curSuccessorIdx >= curOutDegree) {
          // skip nodes with no successors
          do {
            curNode = nodeIt.nextInt()
            curSuccessorArray = nodeIt.successorArray()
            curOutDegree = nodeIt.outdegree()
            pl.lightUpdate()
          } while(curOutDegree == 0 && nodeIt.hasNext)
          curSuccessorIdx = 0
        }
        val succ = curSuccessorArray(curSuccessorIdx)
        curSuccessorIdx += 1
        (new EdgeKey(curNode, succ), 1.0f)
      }
    }
  }

  def kryoFile[T](sc: SparkContext, path: String, minPartitions: Int = 1)(implicit ct: ClassTag[T]): RDD[T] = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => {
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(x._2.getBytes)
      val data = kryo.readClassAndObject(input)
      val dataObject = data.asInstanceOf[Array[T]]
      dataObject
    })

  }

  def blockGraph(sc: SparkContext, path: String) = {
    val attrs: RDD[(Int, NodeAttributesBlock[Vertex])] = kryoFile(sc, new Path(path, "attributes").toString)
    val adjs: RDD[(Int, AdjacencyBlock)] = kryoFile(sc, new Path(path, "adjacencies").toString)
    new BlockGraph[Vertex](attrs, adjs)
  }

}
