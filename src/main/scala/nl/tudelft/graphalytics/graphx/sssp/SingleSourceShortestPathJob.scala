package nl.tudelft.graphalytics.graphx.sssp

import nl.tudelft.graphalytics.graphx.GraphXPregelJob
import nl.tudelft.graphalytics.graphx.GraphXJobOutput
import org.apache.spark.graphx.{VertexId, EdgeTriplet, Graph}
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters

/**
 * Implementation of SSSP in GraphX
 *
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param isDirected the directedness of the graph data
 * @param outputPath the output path of the computation
 * @param parameters the graph-specific parameters for BFS
 * @author Tim Hegeman
 */
class SingleSourceShortestPathJob(graphVertexPath : String, graphEdgePath : String, isDirected : Boolean,
		outputPath : String, parameters : Object)
		extends	GraphXPregelJob[Double, Double, Double](graphVertexPath, graphEdgePath, isDirected, outputPath) {


	val ssspParam : SingleSourceShortestPathsParameters = parameters match {
		case p : SingleSourceShortestPathsParameters => p
		case _ => null
	}

	/**
	 * The SSSP job requires a non-null parameters object of type SSSPParameters.
	 *
	 * @return true iff the input is valid
	 */
	def hasValidInput = ssspParam match {
		case null => false
		case _ => true
	}

	/**
	 * Preprocess the parsed graph (with default vertex and edge values) to a
	 * graph with correct initial values.
	 *
	 * @param graph input graph
	 * @return preprocessed graph
	 */
	override def preprocess(graph : Graph[Double, Double]) =
	  graph.mapVertices((vid, _) =>
	    if (vid == ssspParam.getSourceVertex)
	      0.0
	    else
	      Double.PositiveInfinity)

	/**
	 * Pregel vertex program. Computes a new vertex value based for a given
	 * vertex ID, the old value of the vertex, and aggregated messages.
	 *
	 * For SSSP the new value (distance from the source vertex) is the minimum
	 * of the current value and the smallest incoming message.
	 *
	 * @return the new value of the vertex
	 */
	def vertexProgram = (vertexId : VertexId, oldValue : Double, message : Double) =>
		math.min(oldValue, message)

	/**
	 * Pregel message generation. Produces for each edge a set of messages.
	 *
	 * For SSSP a message (a distance to the destination vertex) is only sent if
	 * the new distance is shorter than the distance already stored at the destination
	 * vertex.
	 *
	 * @return a set of messages to send
	 */
	def sendMsg = (edgeData: EdgeTriplet[Double, Double]) =>
		if (edgeData.srcAttr + edgeData.attr < edgeData.dstAttr)
			Iterator((edgeData.dstId, edgeData.srcAttr + edgeData.attr))
		else
			Iterator.empty

	/**
	 * Pregel messasge combiner. Merges two messages for the same vertex to a
	 * single message.
	 *
	 * For SSSP the only relevant message is the one with the shortest distance from
	 * the source, so two messages can be combined by discarding the larger of the two.
	 *
	 * @return the aggregated message
	 */
	def mergeMsg = (a : Double, b : Double) =>
	  math.min(a, b)

	/**
	 * @return initial message to send to all vertices
	 */
	def getInitialMessage =
	  Double.PositiveInfinity

  /**
   * Parse the attributes on the edges.
   *
   * For SSSP, the attribute is a Double which indicates the distance of
   * the edges.
   *
   * @return the edge weight
   */
	override def parseEdgeData(attr : Array[String]) =
	  attr(0).toDouble

	/**
	 * Convert a graph to the output format of this job.
	 *
	 * For SSSP the output format is one vertex per line, ID and value pair.
	 *
	 * @return a GraphXJobOutput object representing the job result
	 */
	def makeOutput(graph : Graph[Double, Double]) =
		new GraphXJobOutput(graph.vertices.map(
		  vertex => s"${vertex._1} ${vertex._2}"
		).cache())

	/**
	 * @return name of the GraphX job
	 */
	def getAppName = "Single Source Shortests Path"
}