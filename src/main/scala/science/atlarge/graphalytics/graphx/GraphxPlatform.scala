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

import java.nio.file.{Path, Paths}

import nl.tudelft.granula.archiver.PlatformArchive
import nl.tudelft.granula.modeller.job.JobModel
import nl.tudelft.granula.modeller.platform.Graphx
import science.atlarge.graphalytics.graphx.pr.PageRankJob
import science.atlarge.graphalytics.graphx.sssp.SingleSourceShortestPathJob
import science.atlarge.graphalytics.domain._
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun
import science.atlarge.graphalytics.report.result.{BenchmarkMetrics, BenchmarkResult}
import science.atlarge.graphalytics.domain.graph.Graph
import science.atlarge.graphalytics.execution.{Platform, PlatformExecutionException}
import science.atlarge.graphalytics.granula.GranulaAwarePlatform
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import science.atlarge.graphalytics.graphx.bfs.BreadthFirstSearchJob
import science.atlarge.graphalytics.graphx.cdlp.CommunityDetectionLPJob
import science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
import science.atlarge.graphalytics.graphx.ffm.ForestFireModelJob
import science.atlarge.graphalytics.graphx.lcc.LocalClusteringCoefficientJob
import science.atlarge.graphalytics.report.result.{BenchmarkMetrics, BenchmarkResult, PlatformBenchmarkResult}
import org.apache.logging.log4j.{LogManager, Logger}
import org.json.simple.JSONObject
import science.atlarge.graphalytics.domain.algorithms.Algorithm
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun
import science.atlarge.graphalytics.domain.graph.Graph
import science.atlarge.graphalytics.execution.PlatformExecutionException
import science.atlarge.graphalytics.granula.GranulaAwarePlatform
import science.atlarge.graphalytics.graphx.bfs.BreadthFirstSearchJob
import science.atlarge.graphalytics.graphx.cdlp.CommunityDetectionLPJob
import science.atlarge.graphalytics.graphx.ffm.ForestFireModelJob
import science.atlarge.graphalytics.graphx.lcc.LocalClusteringCoefficientJob
import science.atlarge.graphalytics.graphx.pr.PageRankJob
import science.atlarge.graphalytics.graphx.sssp.SingleSourceShortestPathJob
import science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
import science.atlarge.graphalytics.report.result.{BenchmarkMetrics, BenchmarkResult, PlatformBenchmarkResult}

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
		val hdfsVertexPath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.v")
		val hdfsEdgePath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.e")

		val fs = FileSystem.get(new Configuration())
		fs.copyFromLocalFile(localVertexPath, hdfsVertexPath)
		fs.copyFromLocalFile(localEdgePath, hdfsEdgePath)
		fs.close()

		pathsOfGraphs += (graph.getName -> (hdfsVertexPath.toUri.getPath, hdfsEdgePath.toUri.getPath))
	}


	def setupGraphPath(graph : Graph) = {

		val hdfsVertexPath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.v")
		val hdfsEdgePath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.e")
		pathsOfGraphs += (graph.getName -> (hdfsVertexPath.toUri.getPath, hdfsEdgePath.toUri.getPath))
	}


	def execute(benchmark : BenchmarkRun) : PlatformBenchmarkResult = {
		val graph = benchmark.getGraph
		val algorithmType = benchmark.getAlgorithm
		val parameters = benchmark.getAlgorithmParameters
		LOG.info("hi i'm here at executeAlg.")
		setupGraphPath(graph)

		LOG.info("hi i'm here at executeAlg 2.")
		try  {
			val (vertexPath, edgePath) = pathsOfGraphs(graph.getName)
			val outPath = s"$hdfsDirectory/$getPlatformName/output/${benchmark.getId}-${algorithmType.name}-${graph.getName}"
			val isDirected = graph.isDirected

			val job = algorithmType match {
				case Algorithm.BFS => new BreadthFirstSearchJob(vertexPath, edgePath, isDirected, outPath, parameters)
				case Algorithm.CDLP => new CommunityDetectionLPJob(vertexPath, edgePath, isDirected, outPath, parameters)
				case Algorithm.WCC => new WeaklyConnectedComponentsJob(vertexPath, edgePath, isDirected, outPath)
				case Algorithm.FFM => new ForestFireModelJob(vertexPath, edgePath, isDirected, outPath, parameters)
				case Algorithm.LCC => new LocalClusteringCoefficientJob(vertexPath, edgePath, isDirected, outPath)
				case Algorithm.PR => new PageRankJob(vertexPath, edgePath, isDirected, outPath, parameters)
				case Algorithm.SSSP => new SingleSourceShortestPathJob(vertexPath, edgePath, isDirected, outPath, parameters)
				case x => throw new IllegalArgumentException(s"Invalid algorithm type: $x")
			}

			LOG.info("hi i'm here at executeAlg 3.")
			if (job.hasValidInput) {
				job.runJob()

				if(benchmark.isOutputRequired){
					val fs = FileSystem.get(new Configuration())
					fs.copyToLocalFile(false, new org.apache.hadoop.fs.Path(outPath),
						new org.apache.hadoop.fs.Path(benchmark.getOutputDir.toAbsolutePath.toString), true)
					fs.close()
				}

				LOG.info("hi i'm here at executeAlg 4.")
				// TODO: After executing the job, any intermediate and output data should be
				// verified and/or cleaned up. This should preferably be configurable.
				new PlatformBenchmarkResult()

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

	def getPlatformName : String = "graphx"


	override def extractMetrics(): BenchmarkMetrics = new BenchmarkMetrics();

	def preprocess(benchmark: BenchmarkRun) {
		GraphXLogger.stopCoreLogging
		GraphXLogger.startPlatformLogging(benchmark.getLogDir.resolve("platform").resolve("driver.logs"))
	}

	def postprocess(benchmarkRun: BenchmarkRun) {
		GraphXLogger.stopPlatformLogging
		GraphXLogger.startCoreLogging
	}

	def prepare(benchmarkRun: BenchmarkRun) {

	}

	def cleanup(benchmarkRun: BenchmarkRun) {
		GraphXLogger.collectYarnLogs(benchmarkRun.getLogDir)
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

