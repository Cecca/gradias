package it.unipd.dei.gradias.io

import java.io.ByteArrayOutputStream

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.gradias.BlockGraph
import it.unipd.dei.gradias.util.Logger.algoLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

object Outputs {

  def writeSequenceFile(file: String, edges: Iterator[(EdgeKey, Float)]): Unit = {
    val conf = new Configuration()
    val writer = SequenceFile.createWriter(conf,
      Writer.file(new Path(file)),
      Writer.keyClass(classOf[EdgeKey]),
      Writer.valueClass(classOf[FloatWritable]),
      Writer.compression(CompressionType.BLOCK)
    )

    var cnt = 0
    val eWeight = new FloatWritable()
    val start = System.currentTimeMillis()
    val pl = new ProgressLogger(algoLogger, "edges")
    pl.start()
    for((eKey, weight) <- edges) {
      pl.lightUpdate()
      eWeight.set(weight)
      writer.append(eKey, eWeight)
      cnt += 1
    }
    pl.stop()
    algoLogger.info(s"Wrote $cnt edges in ${(System.currentTimeMillis() - start) / 1000.0} seconds")
    writer.close()
  }

  def saveAsKryoFile[T: ClassTag](rdd: RDD[T], path: String): Unit = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
      val kryo = kryoSerializer.newKryo()

      val bao = new ByteArrayOutputStream()
      val output = kryoSerializer.newKryoOutput()
      output.setOutputStream(bao)
      kryo.writeClassAndObject(output, splitArray)
      output.close()

      val byteWritable = new BytesWritable(bao.toByteArray)
      (NullWritable.get(), byteWritable)
    }).saveAsSequenceFile(path)
  }

  def saveAsKryoFile[V: ClassTag](graph: BlockGraph[V], path: String): Unit = {
    saveAsKryoFile(graph.attributes, new Path(path, "attributes").toString)
    saveAsKryoFile(graph.adjacencies, new Path(path, "adjacencies").toString)
  }

}
