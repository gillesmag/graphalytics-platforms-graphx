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

import org.apache.hadoop.yarn.client.cli.LogsCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by wlngai on 9-9-15.
 */
public class GraphalyticLogger {


    protected static final Logger LOG = LogManager.getLogger();

    public static void startCoreLogging() {
//        addConsoleAppender("nl.tudelft.graphalytics", coreLogLevel);
    }

    public static void stopCoreLogging() {
//        removeAppender("nl.tudelft.graphalytics");
    }

    protected static void waitInterval(int waitInterval) {
        try {
            TimeUnit.SECONDS.sleep(waitInterval);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getYarnAppIds(Path clientLogPath) {
        List<String> appIds = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(clientLogPath.toFile()))) {

            String line;
            while ((line = br.readLine()) != null) {
                String appId = null;
                if (line.contains("Submitted application")) {
                    for (String word : line.split("\\s+")) {
                        appId = word;
                    }
                    appIds.add(appId);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (appIds.size() == 0) {
            LOG.error("Failed to find any yarn application ids in the driver log.");
        }
        return appIds;
    }


    public static void collectYarnLog(String applicationId, String yarnlogPath) {

        try {

            PrintStream console = System.out;

            File file = new File(yarnlogPath);
            FileOutputStream fos = new FileOutputStream(file);
            PrintStream ps = new PrintStream(fos);
            System.setOut(ps);
            waitInterval(20);

            LogsCLI logDumper = new LogsCLI();
            logDumper.setConf(new YarnConfiguration());

            String[] args = {"-applicationId", applicationId};

            logDumper.run(args);
            System.setOut(console);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void collectYarnLogs(Path logDataPath) {
        List<String> appIds = GraphalyticLogger.getYarnAppIds(logDataPath.resolve("platform").resolve("driver.logs"));
        for (String appId : appIds) {
            GraphalyticLogger.collectYarnLog(appId, logDataPath + "/platform/yarn" + appId + ".logs");
        }

    }

}