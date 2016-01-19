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
package nl.tudelft.graphalytics.graphx.cd

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.algorithms.cd.{CommunityDetectionOutput, CommunityDetectionValidationTest}

/**
 * Integration test for Community Detection job on GraphX.
 *
 * @author Tim Hegeman
 */
class CommunityDetectionJobTest extends CommunityDetectionValidationTest with GraphXJobTest {

	override def executeDirectedCommunityDetection(graph : GraphStructure, parameters : CommunityDetectionParameters)
	: CommunityDetectionOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToEdgeList(graph)
		executeCommunityDetection(vertexData, edgeData, true, parameters)
	}

	override def executeUndirectedCommunityDetection(graph : GraphStructure, parameters : CommunityDetectionParameters)
	: CommunityDetectionOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToEdgeList(graph)
		executeCommunityDetection(vertexData, edgeData, false, parameters)
	}

	private def executeCommunityDetection(vertexData : List[String], edgeData : List[String], directed : Boolean,
			parameters : CommunityDetectionParameters) : CommunityDetectionOutput = {
		val cdJob = new CommunityDetectionJob("", "", new GraphFormat(directed), "", parameters)
		val (vertexOutput, _) = executeJob(cdJob, vertexData, edgeData)
		val outputAsJavaMap = new util.HashMap[java.lang.Long, java.lang.Long](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => outputAsJavaMap.put(vid, value._3.get._1) }
		new CommunityDetectionOutput(outputAsJavaMap)
	}

}
