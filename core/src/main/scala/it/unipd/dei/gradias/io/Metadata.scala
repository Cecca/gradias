package it.unipd.dei.gradias.io

import java.util.Properties

import it.unipd.dei.gradias.util.Logger.algoLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.collection.mutable

object Metadata {

  def forFile(file: String) = {
    val name = metadataFilePath(file)
    val dataFile = new Path(file)
    val conf = new Configuration()
    new Metadata(name, dataFile, conf)
  }

  private def metadataFilePath(file: String): Path = {
    val fname = new Path(file).getName

    new Path(
      new Path(file).getParent,
      new Path("."+fname+".properties"))
  }

}

class Metadata private (val name: Path, val dataFile: Path, val conf: Configuration) {

  private val fs = name.getFileSystem(conf)

  private val meta = {
    val m = mutable.Map[String, String]()
    if (fs.isFile(name)) {
      val props = new Properties()
      val in = fs.open(name)
      props.load(in)
      val keys = props.keys()
      while (keys.hasMoreElements) {
        val k: String = keys.nextElement().toString
        m(k) = props.getProperty(k)
      }
      in.close()
    }
    m
  }

  def save(): Unit = {
    algoLogger.info(s"Saving metadata file $name")
    val out = fs.create(name, true)
    val props = new Properties()
    for ((k, v) <- meta) {
      props.setProperty(k, v)
    }
    props.store(out, "")
    out.close()
  }

  def get(key: String): Option[String] = meta.get(key)

  def getOrElse(key: String, default: String): String = meta.getOrElse(key, default)

  def set(key: String, value: String): Unit = meta(key) = value

  def setAll(other: Map[String, Any]): Unit = {
    for((k, v) <- other) {
      this.set(k, v.toString)
    }
  }

  def toMap: Map[String, String] = meta.toMap

}
