/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.graphx

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Edge, VertexId, Graph}
import scala.reflect.ClassTag

/**
 * Utility class for loading graphs in various formats (directed vs undirected, vertex- vs edge-based).
 */
object GraphLoader {

	/**
	 * @param vertexData raw vertex data in Graphalytics vertex/edge format
	 * @param edgeData raw edge data in Graphalytics vertex/edge format
	 * @param isDirected the directedness of graph data
	 */
	def loadGraph[VD : ClassTag, ED : ClassTag](vertexData : RDD[String], edgeData : RDD[String],
	    vertexParser : Array[String] => VD, edgeParser : Array[String] => ED, isDirected : Boolean) = {
		val vertices = loadVerticesData(vertexData, vertexParser)
		val edges = if (isDirected)
		              loadEdgesFromEdgeDirectedData(edgeData, edgeParser)
                else
                  loadEdgesFromEdgeUndirectedData(edgeData, edgeParser)

		Graph(vertices, edges)
	}

	/**
	 * @param graphData graphData, vertex-based format
	 * @param parser function that parses attribute of edge
	 * @return a set of vertices
	 */
	private def loadVerticesData[VD : ClassTag](data : RDD[String], parser : Array[String] => VD) : RDD[(VertexId, VD)] = {
	  data.map(line => {
	    val tokens = line.trim.split("""\s""")
	    (tokens(0).toLong, parser(tokens.tail))
	  })
	}

	/**
	 * @param graphData graph data in directed, edge-based format
	 * @param parser function that parses attribute of edge
	 * @return a set of parsed edges
	 */
	private def loadEdgesFromEdgeDirectedData[ED : ClassTag](data : RDD[String], parser : Array[String] => ED) : RDD[Edge[ED]] = {
		data.map(line => {
			val tokens = line.trim.split("""\s""")
			Edge(tokens(0).toLong, tokens(1).toLong, parser(tokens.drop(2)))
		})
	}

	/**
	 * @param graphData graph data in undirected, edge-based format
	 * @param parser function that parses attribute of edge
	 * @return a set of parsed edges
	 */
	private def loadEdgesFromEdgeUndirectedData[ED : ClassTag](data : RDD[String], parser : Array[String] => ED) : RDD[Edge[ED]] = {
		data.flatMap(line => {
			val tokens = line.trim.split("""\s""")
			Array(
			    Edge(tokens(0).toLong, tokens(1).toLong, parser(tokens.drop(2))),
			    Edge(tokens(1).toLong, tokens(0).toLong, parser(tokens.drop(2)))
	    )
		})
	}

}