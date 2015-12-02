package nl.tudelft.graphalytics.graphx.conn

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.graphx.{GraphXJobTest, ValidationGraphUtils}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.conn.{ConnectedComponentsOutput, ConnectedComponentsValidationTest}

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
