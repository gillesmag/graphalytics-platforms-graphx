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
package nl.tudelft.graphalytics.graphx.ffm

import java.util
import java.lang.{Long => JLong}

import nl.tudelft.graphalytics.domain.algorithms.ForestFireModelParameters
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.algorithms.ffm.ForestFireModelValidationTest

/**
 * Integration test for Forest Fire Model job on GraphX.
 *
 * @author Tim Hegeman
 */
class ForestFireModelJobTest extends ForestFireModelValidationTest with GraphXJobTest {

	override def executeDirectedForestFireModel(graph : GraphStructure, parameters : ForestFireModelParameters)
	: GraphStructure = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToVertexEdgeList(graph)
		executeForestFireModel(vertexData, edgeData, true, parameters)
	}

	override def executeUndirectedForestFireModel(graph : GraphStructure, parameters : ForestFireModelParameters)
	: GraphStructure = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToVertexEdgeList(graph)
		executeForestFireModel(vertexData, edgeData, false, parameters)
	}

	private def executeForestFireModel(vertexData : List[String], edgeData : List[String], directed : Boolean,
			parameters : ForestFireModelParameters) : GraphStructure = {
		val ffmJob = new ForestFireModelJob("", "", directed, "", parameters)
		val (vertexOutput, edgeOutput) = executeJob(ffmJob, vertexData, edgeData)
		val edgeLists = new util.HashMap[JLong, util.Set[JLong]]()
		vertexOutput.foreach { case (vid, _) => edgeLists.put(vid, new util.HashSet[JLong]())}
		edgeOutput.foreach { case (from, to) => edgeLists.get(from).add(to) }
		new GraphStructure(edgeLists)
	}

}
