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

package it.unipd.dei.gradias.serialization

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.AllScalaRegistrar
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unipd.dei.gradias.diameter.CSCLocalGraph
import it.unipd.dei.gradias.diameter.dijkstra.DijkstraGraph
import it.unipd.dei.gradias.weighted.decomposition.{DeltaCluster, GraphxClusterInfo}
import it.unipd.dei.gradias.{AdjacencyBlock, Distance, NodeAttributesBlock, NodeId}
import org.apache.spark.serializer.KryoRegistrator
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Trait that enables kryo serialization and registers some classes
 */
trait KryoSerialization {

  val registratorName = "it.unipd.dei.gradias.serialization.GraphKryoRegistrator"

}

class GraphKryoRegistrator extends KryoRegistrator {

  private val log = LoggerFactory.getLogger("KryoRegistrator")

  override def registerClasses(kryo: Kryo) {

    new AllScalaRegistrar()(kryo)

    kryo.setRegistrationRequired(false)

    val toRegister = List (

      // Classes that need to be serialized to avoid exceptions caused by
      // kryo.setRegistrationRequired(true)
      classOf[scala.runtime.BoxedUnit],
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]],
      classOf[Array[(Int, Int)]],
      classOf[(NodeId, Distance)],
      classOf[Array[Object]],
      classOf[ArrayBuffer[Int]],
      classOf[Array[Int]],
      classOf[Array[Distance]],
      classOf[Array[Array[Distance]]],
      classOf[Array[Array[Int]]],
      classOf[Range],

      // classes of our own implementation
      classOf[DeltaCluster.Vertex],
      classOf[Array[DeltaCluster.Vertex]],
      classOf[DeltaCluster.Message],
      classOf[DijkstraGraph],
      classOf[CSCLocalGraph],
      classOf[AdjacencyBlock],
      classOf[NodeAttributesBlock[DeltaCluster.Vertex]],
      classOf[Int2ObjectOpenHashMap[DeltaCluster.Vertex]],
      classOf[Int2ObjectOpenHashMap[DeltaCluster.Message]],
      classOf[GraphxClusterInfo],
      classOf[Array[GraphxClusterInfo]]
    )

    toRegister.foreach { e =>
      log debug ("registering {}", e.getClass.getName)
      kryo.register(e)
    }

  }

}


