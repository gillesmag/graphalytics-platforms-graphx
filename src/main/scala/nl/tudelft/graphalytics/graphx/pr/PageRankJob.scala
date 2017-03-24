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
package nl.tudelft.graphalytics.graphx.pr

import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters
import nl.tudelft.graphalytics.graphx.{GraphXJob, GraphXJobOutput}
import org.apache.spark.graphx.{TripletFields, Graph}

/**
 * The implementation of PageRank on GraphX. Inspired by GraphX's implementation of PageRank
 * ({@link org.apache.spark.graphx.lib.PageRank}), with the addition of support for "dangling nodes", i.e. nodes
 * without outgoing edges.
 *
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param isDirected the directedness of the graph data
 * @param outputPath the output path of the computation
 * @param parameters the graph-specific parameters for PageRank
 * @author Tim Hegeman
 */
class PageRankJob(graphVertexPath : String, graphEdgePath : String, isDirected : Boolean, outputPath : String,
		parameters : Object)
		extends GraphXJob[Double, Unit](graphVertexPath, graphEdgePath, isDirected, outputPath) {

	val prParam : PageRankParameters = parameters match {
		case p : PageRankParameters => p
		case _ => null
	}

	/**
	  * Perform the graph computation using job-specific logic.
	  *
	  * @param graph the parsed graph with default vertex and edge values
	  * @return the resulting graph after the computation
	  */
	override def compute(graph: Graph[Double, Unit]): Graph[Double, Unit] = {
		// Compute for each edge its "weight", i.e. the fraction of the value of the source that is sent
		// to the destination vertex in each iteration. Also initialize the value of each vertex

		val vertexCount = graph.numVertices;
		val vertexOutDegrees = graph.outerJoinVertices(graph.outDegrees)((_, _, degree) => degree.getOrElse(0))
		val weightedGraph = vertexOutDegrees.mapTriplets(1.0 / _.srcAttr)
		var workGraph = weightedGraph.mapVertices((_, _) => 1.0 / vertexCount).cache()

		// Cache the set of vertices that have no outgoing edges
		val danglingVertices = graph.vertices.minus(graph.outDegrees.mapValues(_ => 0.0)).cache()

		// Perform a fixed number of iterations of the PageRank Algorithm
		var iteration = 0
		while (iteration < prParam.getNumberOfIterations) {
			// Store a reference to the cached result of the previous iteration
			val prevGraph = workGraph

			// Compute the sum of PageRank values of neighbours for every vertex
			val sumOfValues = workGraph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
				_ + _, TripletFields.Src)

			// Compute the sum of all PageRank values of "dangling nodes" in the graph
			val danglingSum = workGraph.vertices.innerJoin(danglingVertices)((_, value, _) => value)
					.aggregate(0.0)((sum, vertexPair) => sum + vertexPair._2, _ + _)

			// Compute the new PageRank value of all nodes
			workGraph = workGraph.outerJoinVertices(sumOfValues)((_, _, newSumOfValues) =>
				(1 - prParam.getDampingFactor) / vertexCount +
				prParam.getDampingFactor * (newSumOfValues.getOrElse(0.0) + danglingSum / vertexCount)).cache()

			// Materialise the working graph
			workGraph.vertices.count()
			workGraph.edges.count()
			// Unpersist the previous cached graph
			prevGraph.unpersist(false)

			iteration += 1
		}

		// Clean up any cached datasets
		danglingVertices.unpersist(false)

		workGraph.mapEdges(_ => Unit)
	}

	/**
	  * Convert a graph to the output format of this job.
	  *
	  * @return a GraphXJobOutput object representing the job result
	  */
	override def makeOutput(graph: Graph[Double, Unit]) =
		new GraphXJobOutput(graph.vertices.map(vertex => s"${vertex._1} ${vertex._2}").cache())

	/**
	  * @return name of the GraphX job
	  */
	override def getAppName: String = "PageRank"

	/**
	  * @return true iff the input is valid
	  */
	override def hasValidInput: Boolean = prParam != null
}
