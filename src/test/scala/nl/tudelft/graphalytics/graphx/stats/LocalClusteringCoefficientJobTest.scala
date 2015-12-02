package nl.tudelft.graphalytics.graphx.stats

import java.util

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.stats.{LocalClusteringCoefficientOutput, LocalClusteringCoefficientValidationTest}

/**
 * Integration test for Local Clustering Coefficient job on GraphX.
 *
 * @author Tim Hegeman
 */
class LocalClusteringCoefficientJobTest extends LocalClusteringCoefficientValidationTest with GraphXJobTest {

	override def executeDirectedLocalClusteringCoefficient(graph : GraphStructure)
	: LocalClusteringCoefficientOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToEdgeList(graph)
		executeLocalClusteringCoefficient(vertexData, edgeData, true)
	}

	override def executeUndirectedLocalClusteringCoefficient(graph : GraphStructure)
	: LocalClusteringCoefficientOutput = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToEdgeList(graph)
		executeLocalClusteringCoefficient(vertexData, edgeData, false)
	}

	private def executeLocalClusteringCoefficient(vertexData : List[String], edgeData : List[String],
			directed: Boolean) : LocalClusteringCoefficientOutput = {
		val lccJob = new LocalClusteringCoefficientJob("", "", new GraphFormat(directed), "")
		val (vertexOutput, _) = executeJob(lccJob, vertexData, edgeData)
		val outputAsJavaMap = new util.HashMap[java.lang.Long, java.lang.Double](vertexOutput.size)
		vertexOutput.foreach { case (vid, value) => outputAsJavaMap.put(vid, value) }
		new LocalClusteringCoefficientOutput(outputAsJavaMap, vertexOutput.values.sum / vertexOutput.size)
	}

}
