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

import java.nio.file.Path

import nl.tudelft.granula.archiver.PlatformArchive
import nl.tudelft.granula.modeller.job.JobModel
import nl.tudelft.granula.modeller.platform.Graphx
import nl.tudelft.graphalytics.graphx.pr.PageRankJob
import nl.tudelft.graphalytics.graphx.sssp.SingleSourceShortestPathJob
import nl.tudelft.graphalytics.{BenchmarkMetrics, Platform, PlatformExecutionException}
import nl.tudelft.graphalytics.domain._
import nl.tudelft.graphalytics.granula.GranulaAwarePlatform
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import nl.tudelft.graphalytics.graphx.bfs.BreadthFirstSearchJob
import nl.tudelft.graphalytics.graphx.cdlp.CommunityDetectionLPJob
import nl.tudelft.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
import nl.tudelft.graphalytics.graphx.ffm.ForestFireModelJob
import nl.tudelft.graphalytics.graphx.lcc.LocalClusteringCoefficientJob
import org.apache.logging.log4j.{LogManager, Logger}
import org.json.simple.JSONObject

/**
 * Constants for GraphXPlatform
 */
object GraphxPlatform {
	val OUTPUT_REQUIRED_KEY = "benchmark.run.output-required"
	val OUTPUT_DIRECTORY_KEY = "benchmark.run.output-directory"
	val OUTPUT_DIRECTORY = "./output/"
	val HDFS_DIRECTORY_KEY = "hadoop.hdfs.directory"
	val HDFS_DIRECTORY = "graphalytics"

	val CONFIG_PATH = "graphx.properties"
	val CONFIG_JOB_NUM_EXECUTORS = "graphx.job.num-executors"
	val CONFIG_JOB_EXECUTOR_MEMORY = "graphx.job.executor-memory"
	val CONFIG_JOB_EXECUTOR_CORES = "graphx.job.executor-cores"
}

/**
 * Graphalytics Platform implementation for GraphX. Manages the datasets on HDFS and launches the appropriate
 * GraphX jobs.
 */
class GraphxPlatform extends GranulaAwarePlatform {
	import GraphxPlatform._

	val LOG: Logger = LogManager.getLogger
	var pathsOfGraphs : Map[String, (String, String)] = Map()

	/* Parse the GraphX configuration file */
	val config = Properties.fromFile(CONFIG_PATH).getOrElse(Properties.empty())
	System.setProperty("spark.executor.cores", config.getString(CONFIG_JOB_EXECUTOR_CORES).getOrElse("1"))
	System.setProperty("spark.executor.memory", config.getString(CONFIG_JOB_EXECUTOR_MEMORY).getOrElse("2g"))
	System.setProperty("spark.executor.instances", config.getString(CONFIG_JOB_NUM_EXECUTORS).getOrElse("1"))

	val outputDirectory = config.getString(OUTPUT_DIRECTORY_KEY).getOrElse(OUTPUT_DIRECTORY)
	val hdfsDirectory = config.getString(HDFS_DIRECTORY_KEY).getOrElse(HDFS_DIRECTORY)
	val outputRequired = config.getString(OUTPUT_REQUIRED_KEY).getOrElse("false")

	def uploadGraph(graph : Graph) = {
		val localVertexPath = new org.apache.hadoop.fs.Path(graph.getVertexFilePath)
		val localEdgePath = new org.apache.hadoop.fs.Path(graph.getEdgeFilePath)
		val hdfsVertexPath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getName/input/${graph.getName}.v")
		val hdfsEdgePath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getName/input/${graph.getName}.e")

		val fs = FileSystem.get(new Configuration())
		fs.copyFromLocalFile(localVertexPath, hdfsVertexPath)
		fs.copyFromLocalFile(localEdgePath, hdfsEdgePath)
		fs.close()

		pathsOfGraphs += (graph.getName -> (hdfsVertexPath.toUri.getPath, hdfsEdgePath.toUri.getPath))
	}


	def setupGraphPath(graph : Graph) = {

		val hdfsVertexPath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getName/input/${graph.getName}.v")
		val hdfsEdgePath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getName/input/${graph.getName}.e")
		pathsOfGraphs += (graph.getName -> (hdfsVertexPath.toUri.getPath, hdfsEdgePath.toUri.getPath))
	}


	def executeAlgorithmOnGraph(benchmark : Benchmark) : PlatformBenchmarkResult = {
		val graph = benchmark.getGraph
		val algorithmType = benchmark.getAlgorithm
		val parameters = benchmark.getAlgorithmParameters
		LOG.info("hi i'm here at executeAlg.")
		setupGraphPath(graph)

		LOG.info("hi i'm here at executeAlg 2.")
		try  {
			val (vertexPath, edgePath) = pathsOfGraphs(graph.getName)
			val outPath = s"$hdfsDirectory/$getName/output/${benchmark.getId}-${algorithmType.name}-${graph.getName}"
			val format = graph.getGraphFormat

			val job = algorithmType match {
				case Algorithm.BFS => new BreadthFirstSearchJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.CDLP => new CommunityDetectionLPJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.WCC => new WeaklyConnectedComponentsJob(vertexPath, edgePath, format, outPath)
				case Algorithm.FFM => new ForestFireModelJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.LCC => new LocalClusteringCoefficientJob(vertexPath, edgePath, format, outPath)
				case Algorithm.PR => new PageRankJob(vertexPath, edgePath, format, outPath, parameters)
				case Algorithm.SSSP => new SingleSourceShortestPathJob(vertexPath, edgePath, format, outPath, parameters)
				case x => throw new IllegalArgumentException(s"Invalid algorithm type: $x")
			}

			LOG.info("hi i'm here at executeAlg 3.")
			if (job.hasValidInput) {
				job.runJob()

				if(benchmark.isOutputRequired){
					val fs = FileSystem.get(new Configuration())
					fs.copyToLocalFile(false, new org.apache.hadoop.fs.Path(outPath),
						new org.apache.hadoop.fs.Path(benchmark.getOutputPath), true)
					fs.close()
				}

				LOG.info("hi i'm here at executeAlg 4.")
				// TODO: After executing the job, any intermediate and output data should be
				// verified and/or cleaned up. This should preferably be configurable.
				new PlatformBenchmarkResult(NestedConfiguration.empty())

			} else {
				throw new IllegalArgumentException("Invalid parameters for job")
			}


		} catch {
			case e : Exception => throw new PlatformExecutionException("GraphX job failed with exception: ", e)
		}
	}

	def deleteGraph(graphName : String) = {
		// TODO: Delete graph data from HDFS to clean up. This should preferably be configurable.
	}

	def getName : String = "graphx"

	def getPlatformConfiguration: NestedConfiguration =
		try {
			val configuration: PropertiesConfiguration = new PropertiesConfiguration("graphx.properties")
			NestedConfiguration.fromExternalConfiguration(configuration, "graphx.properties")
		}
		catch {
			case ex: ConfigurationException => NestedConfiguration.empty
		}

	override def retrieveMetrics(): BenchmarkMetrics = new BenchmarkMetrics();

	def preBenchmark(benchmark: Benchmark, path: java.nio.file.Path) {
		GraphXLogger.stopCoreLogging
		GraphXLogger.startPlatformLogging(path.resolve("platform").resolve("driver.logs"))
	}

	def postBenchmark(benchmark: Benchmark, path: java.nio.file.Path) {
		GraphXLogger.collectYarnLogs(path)
		GraphXLogger.stopPlatformLogging
		GraphXLogger.startCoreLogging
	}

	def getJobModel: JobModel = {
		return new JobModel(new Graphx)
	}

	def enrichMetrics(benchmarkResult: BenchmarkResult, arcDirectory: Path) {
		try {
			val platformArchive: PlatformArchive = PlatformArchive.readArchive(arcDirectory)
			val processGraph: JSONObject = platformArchive.operation("ProcessGraph")
			val procTime: Long = platformArchive.info(processGraph, "Duration").toLong
			val metrics: BenchmarkMetrics = benchmarkResult.getMetrics
			metrics.setProcessingTime(procTime)
		}
		catch {
			case e: Exception => {
				LOG.error("Failed to enrich metrics.")
			}
		}
	}
}

