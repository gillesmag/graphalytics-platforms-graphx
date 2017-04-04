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
package science.atlarge.graphalytics.graphx.pr

import java.util

import science.atlarge.graphalytics.domain.algorithms.PageRankParameters
import science.atlarge.graphalytics.graphx.{GraphXJobTest, ValidationGraphUtils}
import science.atlarge.graphalytics.validation.GraphStructure
import science.atlarge.graphalytics.validation.algorithms.pr.{PageRankOutput, PageRankValidationTest}
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters
import science.atlarge.graphalytics.graphx.{GraphXJobTest, ValidationGraphUtils}
import science.atlarge.graphalytics.validation.GraphStructure
import science.atlarge.graphalytics.validation.algorithms.pr.{PageRankOutput, PageRankValidationTest}

/** Integration test for the PageRank job on GraphX.
 *
 * @author Tim Hegeman
 */
class PageRankJobTest extends PageRankValidationTest with GraphXJobTest {

	override def executeDirectedPageRank(graph : GraphStructure, parameters : PageRankParameters) : PageRankOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToVertexEdgeList(graph)
		executePageRank(vertexData, edgeData, true, parameters)
	}

	override def executeUndirectedPageRank(graph : GraphStructure, parameters : PageRankParameters) : PageRankOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToVertexEdgeList(graph)
		executePageRank(vertexData, edgeData, false, parameters)
	}

	private def executePageRank(vertexData : List[String], edgeData : List[String],
			directed: Boolean, parameters : PageRankParameters) : PageRankOutput = {
		val prJob = new PageRankJob("", "", directed, "", parameters)
		val (vertexOutput, _) = executeJob(prJob, vertexData, edgeData)
		val outputAsJavaMap = new util.HashMap[java.lang.Long, java.lang.Double](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => outputAsJavaMap.put(vid, value) }
		new PageRankOutput(outputAsJavaMap)
	}

}
