package nl.tudelft.graphalytics.graphx.cd

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.cd.{CommunityDetectionOutput, CommunityDetectionValidationTest}

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
