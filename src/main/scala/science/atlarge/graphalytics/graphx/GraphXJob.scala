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
package science.atlarge.graphalytics.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{VertexId, Graph, EdgeTriplet}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph

/**
 * Base class for all GraphX jobs in the Graphalytics benchmark. Handles the Spark
 * setup, graph loading, and writing back results.
 *
 * @tparam VD vertex data type
 * @tparam ED edge data type
 * @param graphVertexPath the path of the input graph's vertex data
 * @param graphEdgePath the path of the input graph's edge data
 * @param isDirected the directedness of the graph data
 * @param outputPath the output path of the computation
 */
abstract class GraphXJob[VD : ClassTag, ED : ClassTag](graphVertexPath : String, graphEdgePath : String,
		isDirected : Boolean, outputPath : String) extends Serializable {

	/**
	 * Executes the full GraphX job by reading and parsing the input graph,
	 * running the job-specific graph computation, and writing back the result.
	 */
	def runJob() = {
		// Set up the Spark context for use in the GraphX job.
		val sparkConfiguration = new SparkConf()
			.setAppName(s"GraphalyticsBenchmark: $getAppName")
			.setMaster("yarn")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.ui.showConsoleProgress", "false")
			.set("spark.submit.deployMode", "client")
			.setJars(Seq("/Users/gm/vu/thesis/benchmark/run/graphalytics-1.9.0-SNAPSHOT-graphx-0.2-SNAPSHOT/lib/graphalytics-platforms-graphx-0.2-SNAPSHOT-default.jar"))

		val sparkContext = new SparkContext(sparkConfiguration)

		// Load the raw graph data
		val vertexData : RDD[String] = sparkContext.textFile(graphVertexPath)
		val edgeData : RDD[String] = sparkContext.textFile(graphEdgePath)

		// Execute the job
		val result = executeOnGraph(vertexData, edgeData)

		// Create the output
		val output = makeOutput(result)

		// Write the result
		output.writeToPath(outputPath)

		// Clean up
		result.unpersistVertices(blocking = false)
		result.edges.unpersist(blocking = false)
		output.cleanUp()
		sparkContext.stop()
	}

	/**
	 * Executes the GraphX job using a given graph (as a Spark RDD) and returns the result of the job.
	 *
	 * @param vertexData the vertices of the input graph as a Spark RDD
	 * @param edgeData the edges of the input graph as a Spark RDD
	 * @return the output of the job
	 */
	def executeOnGraph(vertexData : RDD[String], edgeData : RDD[String]) : Graph[VD, ED] = {
		// Parse the vertex and edge data
		val graph = GraphLoader.loadGraph(vertexData, edgeData,
			parseVertexData, parseEdgeData, isDirected).cache()

		println("Vertex count: " + graph.vertices.count())
		println("Edge count: " + graph.edges.count())
		println("ProcessGraph StartTime " + System.currentTimeMillis())

		// Run the graph computation
		val output = compute(graph).cache()

		// Materialize the output and clean up the original graph
		println("Vertex count: " + output.vertices.count())
		println("Edge count: " + output.edges.count())
		println("ProcessGraph EndTime " + System.currentTimeMillis())

		graph.unpersistVertices(blocking = false)
		graph.edges.unpersist(blocking = false)

		output
	}

	/**
	 * Parses the attributes of a vertex (stored as an array of strings).
	 *
	 * Default method simply returns null, subclasses can overwrite this method
	 * to do anything useful.
	 *
	 * @param attr The attributes of the vertex
	 * @return The value of the vertex
	 */
	def parseVertexData(attrs : Array[String]) : VD =
	  null.asInstanceOf[VD]

	/**
	 * Parses the attributes of an edge (stored as an array of strings).
	 *
	 * Default method simply returns null, subclasses can overwrite this method
	 * to do anything useful.
	 *
	 * @param attr The attributes of the edge
	 * @return The value of the edge
	 */
	def parseEdgeData(attr : Array[String]) : ED =
	  null.asInstanceOf[ED]

	/**
	 * Perform the graph computation using job-specific logic.
	 *
	 * @param graph the parsed graph with default vertex and edge values
	 * @return the resulting graph after the computation
	 */
	def compute(graph : Graph[VD, ED]) : Graph[VD, ED]

	/**
	 * Convert a graph to the output format of this job.
	 *
	 * @param graph the graph to output
	 * @return a GraphXJobOutput object representing the job result
	 */
	def makeOutput(graph : Graph[VD, ED]) : GraphXJobOutput

	/**
	 * @return true iff the input is valid
	 */
	def hasValidInput : Boolean

	/**
	 * @return name of the GraphX job
	 */
	def getAppName : String
}
