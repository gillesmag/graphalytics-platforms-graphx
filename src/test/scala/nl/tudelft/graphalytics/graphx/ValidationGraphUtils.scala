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
