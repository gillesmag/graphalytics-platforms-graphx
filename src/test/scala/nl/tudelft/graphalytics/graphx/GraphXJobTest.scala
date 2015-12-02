package nl.tudelft.graphalytics.graphx

import org.apache.spark.SparkContext

import scala.reflect.ClassTag

/**
 * Trait for GraphXJob tests.
 *
 * @author Tim Hegeman
 */
trait GraphXJobTest {

	def executeJob[VD : ClassTag, ED : ClassTag](job : GraphXJob[VD, ED], vertexData : List[String],
			edgeData : List[String]) : (Map[Long, VD], Set[(Long, Long)]) = {
		var sc: SparkContext = null
		try {
			sc = new SparkContext("local", "Graphalytics unit test")
			val vertexRdd = sc.parallelize(vertexData)
			val edgeRdd = sc.parallelize(edgeData)
			val output = job.executeOnGraph(vertexRdd, edgeRdd)
			(output.vertices.collect().toMap, output.edges.map(e => (e.srcId, e.dstId)).collect().toSet)
		} finally {
			if (sc != null) {
				sc.stop()
			}
		}
	}

}
