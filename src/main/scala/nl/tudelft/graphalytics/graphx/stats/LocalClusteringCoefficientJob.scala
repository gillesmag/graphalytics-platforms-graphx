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
package nl.tudelft.graphalytics.graphx.stats

import nl.tudelft.graphalytics.domain.GraphFormat
import org.apache.spark.graphx._
import nl.tudelft.graphalytics.graphx.{GraphXJobOutput, GraphXJob}

/**
 * The implementation of the stats (LCC) algorithm on GraphX. Inspired by the TriangleCount implementation bundled
 * with GraphX.
 *
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param graphFormat the format of the graph data
 * @param outputPath the output path of the computation
 * @author Tim Hegeman
 */
class LocalClusteringCoefficientJob(graphVertexPath : String, graphEdgePath : String, graphFormat : GraphFormat,
		outputPath : String)
		extends GraphXJob[Double, Int](graphVertexPath, graphEdgePath, graphFormat, outputPath) {

	/**
	 * Computes the local clustering coefficient (LCC) for each vertex in the graph.
	 *
	 * @param graph the parsed graph with default vertex and edge values
	 * @return the resulting graph after the computation
	 */
	override def compute(graph: Graph[Boolean, Int]): Graph[Double, Int] = {
		// Deduplicate the edges to ensure that every pair of connected vertices is compared exactly once.
		// The value of an edge represents if the edge is unidirectional (1) or bidirectional (2) in the input graph.
		val canonicalGraph : Graph[Boolean, Int] = graph.mapEdges(_ => 1).convertToCanonicalEdges(_ + _).cache()

		// Collect for each vertex a map of (neighbour, edge value) pairs in either direction in the canonical graph
		val neighboursPerVertex = canonicalGraph.collectEdges(EdgeDirection.Either).mapValues((vid, edges) =>
				edges.map(edge => (edge.otherVertexId(vid), edge.attr)).toMap)

		// Attach the neighbourhood maps as values to the canonical graph
		val neighbourGraph : Graph[Map[VertexId, Int], Int] = canonicalGraph.outerJoinVertices(neighboursPerVertex)(
			(_, _, neighboursOpt) => neighboursOpt.getOrElse(Map[VertexId, Int]())
		).cache()
		// Unpersist the original canonical graph
		canonicalGraph.unpersist(blocking = false)

		// Define the edge-based "map" function
		def edgeToCounts(ctx : EdgeContext[Map[VertexId, Int], Int, Long]) = {
			var countSrc = 0L
			var countDst = 0L
			if (ctx.srcAttr.size < ctx.dstAttr.size) {
				val iter = ctx.srcAttr.iterator
				while (iter.hasNext) {
					val neighbourPair = iter.next()
					countSrc += ctx.dstAttr.getOrElse(neighbourPair._1, 0)
					if (ctx.dstAttr.contains(neighbourPair._1)) {
						countDst += neighbourPair._2
					}
				}
			} else {
				val iter = ctx.dstAttr.iterator
				while (iter.hasNext) {
					val neighbourPair = iter.next()
					countDst += ctx.srcAttr.getOrElse(neighbourPair._1, 0)
					if (ctx.srcAttr.contains(neighbourPair._1)) {
						countSrc += neighbourPair._2
					}
				}
			}
			ctx.sendToSrc(countSrc)
			ctx.sendToDst(countDst)
		}
		// Aggregate messages for all vertices in the map
		val triangles = neighbourGraph.aggregateMessages(edgeToCounts, (a : Long, b : Long) => a + b)
		val triangleCountGraph = neighbourGraph.outerJoinVertices(triangles)(
			(_, _, triangleCountOpt) => triangleCountOpt.getOrElse(0L) / 2)

		// Compute the number of neighbours each vertex has
		val neighbourCounts = neighbourGraph.collectNeighbors(EdgeDirection.Either).mapValues(_.length)
		val lccGraph = triangleCountGraph.outerJoinVertices(neighbourCounts)(
			(_, triangleCount, neighbourCountOpt) => {
				val neighbourCount = neighbourCountOpt.getOrElse(0)
				if (neighbourCount < 2) 0.0
				else triangleCount.toDouble / neighbourCount / (neighbourCount - 1)
			}
		).cache()

		// Materialize the result
		lccGraph.vertices.count()
		lccGraph.edges.count()

		// Unpersist the canonical graph
		canonicalGraph.unpersistVertices(blocking = false)
		canonicalGraph.edges.unpersist(blocking = false)

		lccGraph
	}

	/**
	 * Outputs the mean local clustering coefficient of the graph.
	 *
	 * @param graph the graph to output
	 * @return a GraphXJobOutput object representing the job result
	 */
	override def makeOutput(graph: Graph[Double, Int]) = {
		new GraphXJobOutput(
			graph.vertices.map(vertex => s"${vertex._1} ${vertex._2}").cache()
		)
	}

	/**
	 * @return name of the GraphX job
	 */
	override def getAppName: String = "Local Clustering Coefficient"

	/**
	 * @return true iff the input is valid
	 */
	override def hasValidInput: Boolean = true

}
