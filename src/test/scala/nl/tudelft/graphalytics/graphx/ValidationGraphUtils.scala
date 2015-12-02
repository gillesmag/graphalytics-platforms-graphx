package nl.tudelft.graphalytics.graphx

import nl.tudelft.graphalytics.validation.GraphStructure
import scala.collection.JavaConversions._

/**
 * Created by tim on 12/2/15.
 */
object ValidationGraphUtils {

	def undirectedValidationGraphToEdgeList(graphData : GraphStructure) : (List[String], List[String]) = {
		val orderedVertices = graphData.getVertices.toList.sorted
		val vertices = orderedVertices.map(_.toString)
		val edges = orderedVertices.flatMap(vertex => graphData.getEdgesForVertex(vertex)
				.filter(neighbour => neighbour > vertex).toList.sorted
				.map(neighbour => s"$vertex $neighbour"))
		(vertices, edges)
	}

	def directedValidationGraphToEdgeList(graphData : GraphStructure) : (List[String], List[String]) = {
		val orderedVertices = graphData.getVertices.toList.sorted
		val vertices = orderedVertices.map(_.toString)
		val edges = orderedVertices.flatMap(vertex => graphData.getEdgesForVertex(vertex).toList.sorted
				.map(neighbour => s"$vertex $neighbour"))
		(vertices, edges)
	}

}
