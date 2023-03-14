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
package science.atlarge.graphalytics.graphx;

import org.apache.hadoop.yarn.client.cli.LogsCLI;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.logging.log4j.LogManager;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by wlngai on 9-9-15.
 */
public class GraphXLogger {


    protected static final org.apache.logging.log4j.Logger LOG = LogManager.getLogger();

    private static PrintStream console;
    protected static Level platformLogLevel = Level.INFO;

    public static void startPlatformLogging(Path fileName) {

        console = System.out;
        try {
            File file = null;
            file = fileName.toFile();
            file.getParentFile().mkdirs();
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);
            PrintStream ps = new PrintStream(fos);
            System.setOut(ps);
        } catch(Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("cannot redirect to output file");
        }
        System.out.println("StartTime: " + System.currentTimeMillis());

        Logger.getRootLogger().removeAllAppenders();
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile(fileName.toString() + "-graphaltyics");
        fa.setLayout(new PatternLayout("%d [%t] %-5p[%c{1} (%M(%L))] %m%n"));
        fa.setThreshold(platformLogLevel);
        fa.setAppend(true);
        fa.activateOptions();
        Logger.getRootLogger().addAppender(fa);
        waitInterval(1);
    }

    public static void stopPlatformLogging() {
        Logger.getRootLogger().removeAllAppenders();

        System.out.println("EndTime: " + System.currentTimeMillis());
        System.setOut(console);
    }



    public static void startCoreLogging() {
//        addConsoleAppender("science.atlarge.graphalytics", coreLogLevel);
    }

    public static void stopCoreLogging() {
//        removeAppender("science.atlarge.graphalytics");
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
                if (line.contains("Submitted application") && line.contains("YarnClientImpl")) {
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
        List<String> appIds = getYarnAppIds(logDataPath.resolve("platform").resolve("driver.logs-graphaltyics"));
        for (String appId : appIds) {
            System.out.println("Collecting yarn logs for application " + logDataPath);
            System.out.println("Collecting yarn logs for application " + appId);
            collectYarnLog(appId, logDataPath + "/platform/yarn" + appId + ".logs");
        }

    }


}
