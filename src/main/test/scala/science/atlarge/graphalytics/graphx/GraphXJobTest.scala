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
			sc.setLogLevel("WARN")
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
