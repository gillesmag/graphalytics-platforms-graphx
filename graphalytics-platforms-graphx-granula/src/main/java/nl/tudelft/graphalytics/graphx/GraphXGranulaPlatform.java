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
package nl.tudelft.graphalytics.graphx;

import nl.tudelft.graphalytics.granula.GranulaAwarePlatform;
import nl.tudelft.graphalytics.granula.GranulaManager;
import nl.tudelft.graphalytics.graphx.reporting.logging.GraphXLogger;
import nl.tudelft.pds.granula.modeller.model.job.JobModel;
import nl.tudelft.pds.granula.modeller.graphx.job.GraphX;
import java.nio.file.Path;

/**
 * GraphX platform integration for the Graphalytics benchmark.
 */
public final class GraphXGranulaPlatform extends GraphXPlatform implements GranulaAwarePlatform {


	@Override
	public void setBenchmarkLogDirectory(Path logDirectory) {
		GraphXLogger.stopCoreLogging();
		if(GranulaManager.isLoggingEnabled) {
			GraphXLogger.startPlatformLogging(logDirectory.resolve("OperationLog").resolve("driver.logs"));
		}
	}

	@Override
	public void finalizeBenchmarkLogs(Path logDirectory) {
		if(GranulaManager.isLoggingEnabled) {
			GraphXLogger.collectYarnLogs(logDirectory);
			GraphXLogger.collectUtilLog(null, null, 0, 0, logDirectory);
			GraphXLogger.stopPlatformLogging();
		}
		GraphXLogger.startCoreLogging();

	}

	@Override
	public JobModel getGranulaModel() {
		return new GraphX();
	}
}
