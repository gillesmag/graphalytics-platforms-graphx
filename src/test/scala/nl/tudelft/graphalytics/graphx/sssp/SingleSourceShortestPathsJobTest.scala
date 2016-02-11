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
package nl.tudelft.graphalytics.graphx.sssp

import java.util
import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.graphx.GraphXJobTest
import nl.tudelft.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsValidationTest
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters
import nl.tudelft.graphalytics.validation.algorithms.bfs.BreadthFirstSearchOutput
import nl.tudelft.graphalytics.util.graph.PropertyGraph
import nl.tudelft.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsOutput
import nl.tudelft.graphalytics.graphx.ValidationGraphUtils

/**
 * Integration test for BFS job on GraphX.
 *
 * @author Tim Hegeman
 */
class SingleSourceShortestPathsJobTest extends SingleSourceShortestPathsValidationTest with GraphXJobTest {

  override def executeDirectedSingleSourceShortestPaths(graph : PropertyGraph[java.lang.Void,java.lang.Double],
      parameters : SingleSourceShortestPathsParameters) : SingleSourceShortestPathsOutput = {
    val (vertexData, edgeData) = ValidationGraphUtils.directedPropertyGraphToVertexEdgeList(graph)
    execute(vertexData, edgeData, true, parameters)
  }

  override def executeUndirectedSingleSourceShortestPaths(graph : PropertyGraph[java.lang.Void,java.lang.Double],
      parameters : SingleSourceShortestPathsParameters) = {
    val (vertexData, edgeData) = ValidationGraphUtils.undirectedPropertyGraphToVertexEdgeList(graph)
    execute(vertexData, edgeData, false, parameters)
  }

  private def execute(vertexData : List[String], edgeData : List[String], directed: Boolean,
			parameters: SingleSourceShortestPathsParameters) = {
    val job = new SingleSourceShortestPathJob("", "", new GraphFormat(directed), "", parameters)
		val (vertexOutput, _) = executeJob(job, vertexData, edgeData)
		val output = new util.HashMap[java.lang.Long, java.lang.Double](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => output.put(vid, value) }
		new SingleSourceShortestPathsOutput(output)
  }
}
