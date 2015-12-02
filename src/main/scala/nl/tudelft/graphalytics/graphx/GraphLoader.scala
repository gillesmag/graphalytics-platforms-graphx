/**
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
package nl.tudelft.graphalytics.graphx

import nl.tudelft.graphalytics.domain.GraphFormat
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
	 * @param graphFormat the graph data format specification
	 * @param defaultValue default value for each vertex
	 * @tparam VD vertex value type
	 * @return a parsed GraphX graph
	 */
	def loadGraph[VD : ClassTag](vertexData : RDD[String], edgeData : RDD[String], graphFormat : GraphFormat,
			defaultValue : VD) : Graph[VD, Int] = {
		val vertices = vertexData.map[(VertexId, VD)](line => (line.toLong, defaultValue))
		val edges = (
				if (graphFormat.isDirected) loadEdgesFromEdgeDirectedData(edgeData)
				else loadEdgesFromEdgeUndirectedData(edgeData)
		).map { case (vid1, vid2) => Edge(vid1, vid2, 0) }
		Graph(vertices, edges)
	}

	/**
	 * @param graphData graph data in directed, edge-based format
	 * @return a set of parsed edges
	 */
	private def loadEdgesFromEdgeDirectedData(graphData : RDD[String]) : RDD[(VertexId, VertexId)] = {
		def lineToEdge(s : String) : (VertexId, VertexId) = {
			val tokens = s.trim.split("""\s""")
			(tokens(0).toLong, tokens(1).toLong)
		}
		
		graphData.map(lineToEdge)
	}

	/**
	 * @param graphData graph data in undirected, edge-based format
	 * @return a set of parsed edges
	 */
	private def loadEdgesFromEdgeUndirectedData(graphData : RDD[String]) : RDD[(VertexId, VertexId)] = {
		def lineToEdges(s : String) : Array[(VertexId, VertexId)] = {
			val tokens = s.trim.split("""\s""")
			Array((tokens(0).toLong, tokens(1).toLong), (tokens(1).toLong, tokens(0).toLong))
		}
		
		graphData.flatMap(lineToEdges)
	}
	
}