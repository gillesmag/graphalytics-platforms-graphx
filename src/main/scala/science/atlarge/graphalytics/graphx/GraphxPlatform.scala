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

import java.math.BigDecimal
import java.nio.file.Path
import java.util
import java.util.List

import scala.collection.JavaConversions._
import science.atlarge.granula.archiver.PlatformArchive
import science.atlarge.granula.modeller.job.JobModel
import science.atlarge.granula.util.FileUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.yarn.client.cli.ApplicationCLI
import org.apache.logging.log4j.{LogManager, Logger}
import org.json.simple.JSONObject
import science.atlarge.granula.modeller.platform.Graphx
import science.atlarge.graphalytics.configuration.GraphalyticsExecutionException
import science.atlarge.graphalytics.domain.algorithms.Algorithm
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun
import science.atlarge.graphalytics.domain.graph.{FormattedGraph, LoadedGraph}
import science.atlarge.graphalytics.execution.{PlatformExecutionException, RunSpecification}
import science.atlarge.graphalytics.granula.GranulaAwarePlatform
import science.atlarge.graphalytics.graphx.bfs.BreadthFirstSearchJob
import science.atlarge.graphalytics.graphx.cdlp.CommunityDetectionLPJob
import science.atlarge.graphalytics.graphx.ffm.ForestFireModelJob
import science.atlarge.graphalytics.graphx.lcc.LocalClusteringCoefficientJob
import science.atlarge.graphalytics.graphx.pr.PageRankJob
import science.atlarge.graphalytics.graphx.sssp.SingleSourceShortestPathJob
import science.atlarge.graphalytics.graphx.wcc.WeaklyConnectedComponentsJob
import science.atlarge.graphalytics.report.result.{BenchmarkMetric, BenchmarkMetrics, BenchmarkRunResult}

/**
 * Constants for GraphXPlatform
 */
object GraphxPlatform {
	val OUTPUT_REQUIRED_KEY = "benchmark.run.output-required"
	val OUTPUT_DIRECTORY_KEY = "benchmark.run.output-directory"
	val OUTPUT_DIRECTORY = "./output/"
	val HDFS_DIRECTORY_KEY = "platform.hadoop.hdfs.directory"
	val HDFS_DIRECTORY = "graphalytics"

	val CONFIG_PATH = "benchmark.properties"
	val CONFIG_JOB_NUM_EXECUTORS = "platform.graphx.job.num-executors"
	val CONFIG_JOB_EXECUTOR_MEMORY = "platform.graphx.job.executor-memory"
	val CONFIG_JOB_EXECUTOR_CORES = "platform.graphx.job.executor-cores"
}

/**
 * Graphalytics Platform implementation for GraphX. Manages the datasets on HDFS and launches the appropriate
 * GraphX jobs.
 */
class GraphxPlatform extends GranulaAwarePlatform {
	import GraphxPlatform._

	val LOG: Logger = LogManager.getLogger

	/* Parse the GraphX configuration file */
	val config = Properties.fromFile(CONFIG_PATH).getOrElse(Properties.empty())
	System.setProperty("spark.executor.cores", config.getString(CONFIG_JOB_EXECUTOR_CORES).getOrElse("1"))
	System.setProperty("spark.executor.memory", config.getString(CONFIG_JOB_EXECUTOR_MEMORY).getOrElse("2g"))
	System.setProperty("spark.executor.instances", config.getString(CONFIG_JOB_NUM_EXECUTORS).getOrElse("1"))

	val outputDirectory = config.getString(OUTPUT_DIRECTORY_KEY).getOrElse(OUTPUT_DIRECTORY)
	val hdfsDirectory = config.getString(HDFS_DIRECTORY_KEY).getOrElse(HDFS_DIRECTORY)
	val outputRequired = config.getString(OUTPUT_REQUIRED_KEY).getOrElse("false")


	def verifySetup(): Unit = {}

	def loadGraph(graph : FormattedGraph) : LoadedGraph = {
		val localVertexPath = new org.apache.hadoop.fs.Path(graph.getVertexFilePath)
		val localEdgePath = new org.apache.hadoop.fs.Path(graph.getEdgeFilePath)
		val hdfsVertexPath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.v")
		val hdfsEdgePath = new org.apache.hadoop.fs.Path(s"$hdfsDirectory/$getPlatformName/input/${graph.getName}.e")

		val fs = FileSystem.get(new Configuration())
		fs.copyFromLocalFile(localVertexPath, hdfsVertexPath)
		fs.copyFromLocalFile(localEdgePath, hdfsEdgePath)
		fs.close()

		return new LoadedGraph(graph, hdfsVertexPath.toString, hdfsEdgePath.toString);
	}

	def deleteGraph(loadedGraph: LoadedGraph) = {
		// TODO: Delete graph data from HDFS to clean up. This should preferably be configurable.
	}

	def prepare(benchmarkSpec: RunSpecification) {

	}

	def startup(benchmarkSpec: RunSpecification) {
		GraphXLogger.stopCoreLogging
		val benchmarkSetup = benchmarkSpec.getBenchmarkRunSetup
		GraphXLogger.startPlatformLogging(benchmarkSetup.getLogDir.resolve("platform").resolve("driver.logs"))
	}

	def run(benchmarkSpec: RunSpecification) = {
		val benchmarkRun = benchmarkSpec.getBenchmarkRun
		val benchmarkRunSetup = benchmarkSpec.getBenchmarkRunSetup
		val runtimeSetup = benchmarkSpec.getRuntimeSetup

		val graph = benchmarkRun.getFormattedGraph
		val algorithmType = benchmarkRun.getAlgorithm
		val parameters = benchmarkRun.getAlgorithmParameters

		try  {
			val vertexPath = runtimeSetup.getLoadedGraph.getVertexPath
			val edgePath = runtimeSetup.getLoadedGraph.getEdgePath
			val outPath = s"$hdfsDirectory/$getPlatformName/output/${benchmarkRun.getId}-${algorithmType.name}-${graph.getName}"
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

			if (job.hasValidInput) {
				job.runJob()

				if(benchmarkRunSetup.isOutputRequired){
					val fs = FileSystem.get(new Configuration())
					fs.copyToLocalFile(false, new org.apache.hadoop.fs.Path(outPath),
						new org.apache.hadoop.fs.Path(benchmarkRunSetup.getOutputDir.toAbsolutePath.toString), true)
					fs.close()
				}

				// TODO: After executing the job, any intermediate and output data should be
				// verified and/or cleaned up. This should preferably be configurable.

			} else {
				throw new IllegalArgumentException("Invalid parameters for job")
			}


		} catch {
			case e : Exception => throw new PlatformExecutionException("GraphX job failed with exception: ", e)
		}
	}

	def finalize(benchmarkSpec: RunSpecification): BenchmarkMetrics = {
		GraphXLogger.stopPlatformLogging
		GraphXLogger.startCoreLogging


		val benchmarkRun = benchmarkSpec.getBenchmarkRun
		val benchmarkSetup = benchmarkSpec.getBenchmarkRunSetup
		GraphXLogger.collectYarnLogs(benchmarkSetup.getLogDir)

		val logs = FileUtil.readFile(benchmarkSetup.getLogDir.resolve("platform").resolve("driver.logs"))

		var startTime = -1l
		var endTime = -1l

		for (line <- logs.split("\n")) {
			try {
				if (line.contains("ProcessGraph StartTime ")) {
					startTime = line.split("\\s+").last.toLong
				}
				if (line.contains("ProcessGraph EndTime ")) {
					endTime = line.split("\\s+").last.toLong
				}
			} catch {
				case e: Exception =>
					LOG.error(String.format("Cannot parse line: %s", line))
					e.printStackTrace()
			}
		}

		if (startTime != -1 && endTime != -1) {
			val metrics = new BenchmarkMetrics
			val procTimeMS = endTime - startTime
			val procTimeS = new BigDecimal(procTimeMS).divide(new BigDecimal(1000), 3, BigDecimal.ROUND_CEILING)
			metrics.setProcessingTime(new BenchmarkMetric(procTimeS, "s"))
			return metrics
		}
		else return new BenchmarkMetrics
	}

	def enrichMetrics(benchmarkResult: BenchmarkRunResult, arcDirectory: Path) {
		try {
			val metrics: BenchmarkMetrics = benchmarkResult.getMetrics
			val platformArchive: PlatformArchive = PlatformArchive.readArchive(arcDirectory)
			val processGraph: JSONObject = platformArchive.operation("ProcessGraph")

			val procTimeMS: Long = platformArchive.info(processGraph, "Duration").toLong
			val procTimeS : BigDecimal = new BigDecimal(procTimeMS).divide(new BigDecimal(1000), 3, BigDecimal.ROUND_CEILING);
			metrics.setProcessingTime(new BenchmarkMetric(procTimeS, "s"));
		}
		catch {
			case e: Exception => {
				LOG.error("Failed to enrich metrics.")
			}
		}
	}

	def terminate(benchmarkSpec: RunSpecification): Unit = {
		val benchmarkSetup = benchmarkSpec.getBenchmarkRunSetup
		val driverPath: Path = benchmarkSetup.getLogDir.resolve("platform").resolve("driver.logs-graphaltyics")
		val appIds: util.List[String] = GraphXLogger.getYarnAppIds(driverPath)

		for (appId <- appIds) {
			LOG.info("Terminating Yarn job: " + appId)
			val args: Array[String] = Array("application", "-kill", appId)
			try {
				val applicationCLI: ApplicationCLI = new ApplicationCLI
				applicationCLI.setSysOutPrintStream(System.out)
				applicationCLI.setSysErrPrintStream(System.err)
				val success: Int = ToolRunner.run(applicationCLI, args)
				applicationCLI.stop()
				if (success == 0) LOG.info("Terminated Yarn job: " + appId)
				else throw new GraphalyticsExecutionException("Failed to terminate task: signal=" + success)
			} catch {
				case e: Exception =>
					throw new GraphalyticsExecutionException("Failed to terminate task", e)
			}
		}
	}

	def getJobModel: JobModel = {
		return new JobModel(new Graphx)
	}

	def getPlatformName : String = "graphx"

}

