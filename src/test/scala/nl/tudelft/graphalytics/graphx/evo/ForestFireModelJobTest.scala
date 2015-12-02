package nl.tudelft.graphalytics.graphx.evo

import java.util
import java.lang.{Long => JLong}

import nl.tudelft.graphalytics.domain.GraphFormat
import nl.tudelft.graphalytics.domain.algorithms.ForestFireModelParameters
import nl.tudelft.graphalytics.graphx.{ValidationGraphUtils, GraphXJobTest}
import nl.tudelft.graphalytics.validation.GraphStructure
import nl.tudelft.graphalytics.validation.evo.ForestFireModelValidationTest

/**
 * Integration test for Forest Fire Model job on GraphX.
 *
 * @author Tim Hegeman
 */
class ForestFireModelJobTest extends ForestFireModelValidationTest with GraphXJobTest {

	override def executeDirectedForestFireModel(graph : GraphStructure, parameters : ForestFireModelParameters)
	: GraphStructure = {
		val (vertexData, edgeData) = ValidationGraphUtils.directedValidationGraphToEdgeList(graph)
		executeForestFireModel(vertexData, edgeData, true, parameters)
	}

	override def executeUndirectedForestFireModel(graph : GraphStructure, parameters : ForestFireModelParameters)
	: GraphStructure = {
		val (vertexData, edgeData) = ValidationGraphUtils.undirectedValidationGraphToEdgeList(graph)
		executeForestFireModel(vertexData, edgeData, false, parameters)
	}

	private def executeForestFireModel(vertexData : List[String], edgeData : List[String], directed : Boolean,
			parameters : ForestFireModelParameters) : GraphStructure = {
		val ffmJob = new ForestFireModelJob("", "", new GraphFormat(directed), "", parameters)
		val (vertexOutput, edgeOutput) = executeJob(ffmJob, vertexData, edgeData)
		val edgeLists = new util.HashMap[JLong, util.Set[JLong]]()
		vertexOutput.foreach { case (vid, _) => edgeLists.put(vid, new util.HashSet[JLong]())}
		edgeOutput.foreach { case (from, to) => edgeLists.get(from).add(to) }
		new GraphStructure(edgeLists)
	}

}
