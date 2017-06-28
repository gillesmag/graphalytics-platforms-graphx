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

import science.atlarge.graphalytics.validation.GraphStructure

import scala.collection.JavaConversions._
import science.atlarge.graphalytics.util.graph.PropertyGraph
import science.atlarge.graphalytics.util.graph.PropertyGraph
import science.atlarge.graphalytics.validation.GraphStructure

/**
 * Utility class for transforming a GraphStructure (adjacency list) representation to a list of vertices and a list of
 * edges.
 *
 * @author Tim Hegeman
 */
object ValidationGraphUtils {

	def undirectedPropertyGraphToVertexEdgeList[V, E](graphData : PropertyGraph[V,E]) : (List[String], List[String]) = {
	  val orderedVertices = graphData.getVertices.toList
	  val vertices = orderedVertices.map(v => s"${v.getId} ${v.getValue}");
	  val edges = orderedVertices.flatMap(vertex => vertex.getOutgoingEdges
	      .filter(e => e.getSourceVertex.getId > e.getDestinationVertex.getId)
	      .map(e => s"${e.getSourceVertex.getId} ${e.getDestinationVertex.getId} ${e.getValue}"))
	  (vertices, edges)
	}

	def directedPropertyGraphToVertexEdgeList[V, E](graphData : PropertyGraph[V,E]) : (List[String], List[String]) = {
	  val orderedVertices = graphData.getVertices.toList
	  val vertices = orderedVertices.map(v => s"${v.getId} ${v.getValue}");
	  val edges = orderedVertices.flatMap(vertex => vertex.getOutgoingEdges
	      .map(e => s"${e.getSourceVertex.getId} ${e.getDestinationVertex.getId} ${e.getValue}"))
	  (vertices, edges)
	}

	def undirectedValidationGraphToVertexEdgeList(graphData : GraphStructure) : (List[String], List[String]) = {
		val orderedVertices = graphData.getVertices.toList.sorted
		val vertices = orderedVertices.map(_.toString)
		val edges = orderedVertices.flatMap(vertex => graphData.getEdgesForVertex(vertex)
				.filter(neighbour => neighbour > vertex).toList.sorted
				.map(neighbour => s"$vertex $neighbour"))
		(vertices, edges)
	}

	def directedValidationGraphToVertexEdgeList(graphData : GraphStructure) : (List[String], List[String]) = {
		val orderedVertices = graphData.getVertices.toList.sorted
		val vertices = orderedVertices.map(_.toString)
		val edges = orderedVertices.flatMap(vertex => graphData.getEdgesForVertex(vertex).toList.sorted
				.map(neighbour => s"$vertex $neighbour"))
		(vertices, edges)
	}

}
