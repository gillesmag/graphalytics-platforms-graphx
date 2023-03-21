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
package science.atlarge.graphalytics.graphx.wcc

import org.apache.spark.graphx.{Graph, VertexId}
import science.atlarge.graphalytics.graphx.GraphXJobOutput
import science.atlarge.graphalytics.graphx.{GraphXJob, GraphXJobOutput}

/**
 * The implementation of weakly connected components on GraphX.
 *
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param isDirected the directedness of the graph data
 * @param outputPath the output path of the computation
 * @author Tim Hegeman
 */
class WeaklyConnectedComponentsJob(graphVertexPath : String, graphEdgePath : String, isDirected : Boolean, outputPath : String)
		extends GraphXJob[VertexId, Unit](graphVertexPath, graphEdgePath, isDirected, outputPath) {

	/**
	 * Perform the graph computation using job-specific logic.
	 *
	 * @param graph the parsed graph with default vertex and edge values
	 * @return the resulting graph after the computation
	 */
	override def compute(graph: Graph[VertexId, Unit]): Graph[VertexId, Unit] =
	  graph.connectedComponents()

	/**
	 * Convert a graph to the output format of this job.
	 *
	 * @return a GraphXJobOutput object representing the job result
	 */
	override def makeOutput(graph: Graph[VertexId, Unit]) =
		new GraphXJobOutput(graph.vertices.map(vertex => s"${vertex._1} ${vertex._2}").cache())

	/**
	 * @return name of the GraphX job
	 */
	override def getAppName: String = "Connected Components"

	/**
	 * @return true iff the input is valid
	 */
	override def hasValidInput: Boolean = true
}
