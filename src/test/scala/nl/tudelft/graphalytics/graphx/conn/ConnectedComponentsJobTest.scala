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
package nl.tudelft.graphalytics.graphx.conn

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.graphx.{GraphXJobTest, ValidationGraphUtils}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.algorithms.conn.{ConnectedComponentsOutput, ConnectedComponentsValidationTest}

/**
 * Integration test for Connected Components job on GraphX.
 *
 * @author Tim Hegeman
 */
class ConnectedComponentsJobTest extends ConnectedComponentsValidationTest with GraphXJobTest {

	override def executeDirectedConnectedComponents(graph : GraphStructure) : ConnectedComponentsOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToEdgeList(graph)
		executeConnectedComponents(vertexData, edgeData, true)
	}

	override def executeUndirectedConnectedComponents(graph : GraphStructure) : ConnectedComponentsOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToEdgeList(graph)
		executeConnectedComponents(vertexData, edgeData, false)
	}

	private def executeConnectedComponents(vertexData : List[String], edgeData : List[String],
			directed: Boolean) : ConnectedComponentsOutput = {
		val ccJob = new ConnectedComponentsJob("", "", new GraphFormat(directed), "")
		val (vertexOutput, _) = executeJob(ccJob, vertexData, edgeData)
		val outputAsJavaMap = new util.HashMap[java.lang.Long, java.lang.Long](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => outputAsJavaMap.put(vid, value) }
		new ConnectedComponentsOutput(outputAsJavaMap)
	}

}
