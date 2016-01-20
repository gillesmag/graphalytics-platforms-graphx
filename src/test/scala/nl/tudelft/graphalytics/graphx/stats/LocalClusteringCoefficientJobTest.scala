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

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.algorithms.stats.{LocalClusteringCoefficientOutput, LocalClusteringCoefficientValidationTest}

/**
 * Integration test for Local Clustering Coefficient job on GraphX.
 *
 * @author Tim Hegeman
 */
class LocalClusteringCoefficientJobTest extends LocalClusteringCoefficientValidationTest with GraphXJobTest {

	override def executeDirectedLocalClusteringCoefficient(graph : GraphStructure)
	: LocalClusteringCoefficientOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToVertexEdgeList(graph)
		executeLocalClusteringCoefficient(vertexData, edgeData, true)
	}

	override def executeUndirectedLocalClusteringCoefficient(graph : GraphStructure)
	: LocalClusteringCoefficientOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToVertexEdgeList(graph)
		executeLocalClusteringCoefficient(vertexData, edgeData, false)
	}

	private def executeLocalClusteringCoefficient(vertexData : List[String], edgeData : List[String],
			directed: Boolean) : LocalClusteringCoefficientOutput = {
		val lccJob = new LocalClusteringCoefficientJob("", "", new GraphFormat(directed), "")
		val (vertexOutput, _) = executeJob(lccJob, vertexData, edgeData)
		val outputAsJavaMap = new util.HashMap[java.lang.Long, java.lang.Double](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => outputAsJavaMap.put(vid, value) }
		new LocalClusteringCoefficientOutput(outputAsJavaMap)
	}

}
