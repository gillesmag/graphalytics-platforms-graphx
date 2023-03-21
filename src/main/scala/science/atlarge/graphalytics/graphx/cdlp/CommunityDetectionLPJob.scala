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
package science.atlarge.graphalytics.graphx.cdlp

import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters
import science.atlarge.graphalytics.graphx.{GraphXJobOutput, GraphXPregelJob}
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters
import science.atlarge.graphalytics.graphx.{GraphXJobOutput, GraphXPregelJob}

/**
 * The implementation of deterministic label propagation on GraphX. Inspired by GraphX's implementation of label
 * propagation {@link org.apache.spark.graphx.lib.LabelPropagation}.
 *
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param isDirected the directedness of the graph data
 * @param outputPath the output path of the computation
 * @param parameters the graph-specific parameters for community detection
 * @author Tim Hegeman
 */
class CommunityDetectionLPJob(graphVertexPath : String, graphEdgePath : String, isDirected : Boolean,
		outputPath : String, parameters : Object)
		extends GraphXPregelJob[VertexId, Unit, Map[VertexId, Long]](graphVertexPath, graphEdgePath, isDirected, outputPath) {

	val cdParam : CommunityDetectionLPParameters = parameters match {
		case p : CommunityDetectionLPParameters => p
		case _ => null
	}

	/**
	 * Preprocess the parsed graph (with default vertex and edge values) to a
	 * graph with correct initial values.
	 *
	 * @param graph input graph
	 * @return preprocessed graph
	 */
	override def preprocess(graph: Graph[VertexId, Unit]) =
		graph.mapVertices((vid, _) => vid)

	/**
	 * @return initial message to send to all vertices
	 */
	override def getInitialMessage: Map[VertexId, Long] = Map[VertexId, Long]()

	/**
	 * Pregel messasge combiner. Merges two messages for the same vertex to a
	 * single message.
	 *
	 * @return the aggregated message
	 */
	override def mergeMsg = (A : Map[VertexId, Long], B : Map[VertexId, Long]) =>
		(A.keySet ++ B.keySet).map(label =>
			label -> (A.getOrElse(label, 0L) + B.getOrElse(label, 0L))
		).toMap

	/**
	 * Pregel vertex program. Computes a new vertex value based for a given
	 * vertex ID, the old value of the vertex, and aggregated messages.
	 *
	 * @return the new value of the vertex
	 */
	override def vertexProgram = (vid : VertexId, vertexData: VertexId, messageData: Map[VertexId, Long]) =>
		messageData.fold((vertexData, 0L))((a, b) =>
			if (a._2 > b._2 || (a._2 == b._2 && a._1 < b._1)) a
			else b
		)._1

	/**
	 * Pregel message generation. Produces for each edge a set of messages.
	 *
	 * @return a set of messages to send
	 */
	override def sendMsg = (edge : EdgeTriplet[VertexId, Unit]) => {
		Iterator((edge.dstId, Map(edge.srcAttr -> 1L)), (edge.srcId, Map(edge.dstAttr -> 1L)))
	}

	/**
	 * Convert a graph to the output format of this job.
	 *
	 * @return a GraphXJobOutput object representing the job result
	 */
	override def makeOutput(graph: Graph[VertexId, Unit]) =
		new GraphXJobOutput(graph.vertices.map(vertex => {
			s"${vertex._1} ${vertex._2}"
		}).cache())

	/**
	 * @return name of the GraphX job
	 */
	override def getAppName: String = "Community Detection"

	/**
	 * @return true iff the input is valid
	 */
	override def hasValidInput: Boolean = cdParam != null

	/**
	 * @return the maximum number of iterations to run the Pregel algorithm for.
	 */
	override def getMaxIterations: Int = cdParam.getMaxIterations

}
