package nl.tudelft.granula.modeller.rule.extraction;

import nl.tudelft.granula.modeller.source.DataStream;
import nl.tudelft.granula.modeller.source.log.Log;
import nl.tudelft.granula.modeller.source.log.LogLocation;
import nl.tudelft.granula.util.UuidGenerator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by wing on 21-8-15.
 */
public class GraphXExtractionRule extends ExtractionRule {

    public GraphXExtractionRule(int level) {
        super(level);
    }

    @Override
    public boolean execute() {
        return false;
    }

    public String generateText(String infoName, String infoValue, String actorType, String actorId, String missionType,
                               String missionId, String operationUuid) {
        String text = String.format("GRANULA - InfoName:%s InfoValue:%s ActorType:%s ActorId:%s MissionType:%s MissionId:%s RecordUuid:%s OperationUuid:%s Timestamp:%s\n",
                infoName, infoValue, actorType, actorId, missionType,
                missionId, UuidGenerator.getRandomUUID(), operationUuid, System.currentTimeMillis());
        return text;
    }

    public List<Log> extractLogFromInputStream(DataStream dataStream) {

        List<Log> granularlogList = new ArrayList<>();

        try {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(dataStream.getInputStream()));

            String line = null;
            int lineCount = 0;

            boolean processingStarted = false;
            while ((line = br.readLine()) != null) {
                lineCount++;

                if(line.contains("ProcessGraph StartTime") || line.contains("ProcessGraph EndTime")) {
                    String reformattedLine = reformatLine(line);
                    processingStarted = true;


                    Log log = extractRecord(reformattedLine);

                    LogLocation trace = new LogLocation();

                    String codeLocation;
                    String logFilePath;
                    if(false) { //TODO if supported
                        codeLocation = reformattedLine.split("\\) - Granular")[0].split(" \\(")[1];
                    }

                    codeLocation = "unspecified";
                    logFilePath = "unspecified";

                    trace.setLocation(logFilePath, lineCount, codeLocation);
                    log.setLocation(trace);

                    granularlogList.add(log);
                }



                if(line.contains("GRANULA") ) {

                    if(processingStarted && line.contains("MissionType:Stage")) {
                        line = line.replaceAll("MissionType:Stage", "MissionType:ProcessStage");
                    }

//                    if(line.contains("GraphX") && line.contains("Job")) {
//                        continue;
//                    }

                    Log log = extractRecord(line);

                    LogLocation trace = new LogLocation();

                    String codeLocation;
                    String logFilePath;
                    if(false) { //TODO if supported
                        codeLocation = line.split("\\) - Granular")[0].split(" \\(")[1];
                    }

                    codeLocation = "unspecified";
                    logFilePath = "unspecified";

                    trace.setLocation(logFilePath, lineCount, codeLocation);
                    log.setLocation(trace);

                    granularlogList.add(log);
                }
            }

            br.close();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return granularlogList;
    }

    public String reformatLine(String line) {
        Log log = new Log();

        String[] logAttrs = line.split("\\s+");

        String infoName = logAttrs[1];
        String infoValue = logAttrs[2];

        String reformattedLine = String.format("GRANULA - InfoName:%s InfoValue:%s ActorType:GraphX ActorId:Id.Unique MissionType:ProcessGraph MissionId:Id.Unique Timestamp:%s Extra:None",
                infoName, infoValue, infoValue);

        return reformattedLine;
    }

    public Log extractRecord(String line) {
        Log log = new Log();

        String granularLog = line.split("GRANULA ")[1];
        String[] logAttrs = granularLog.split("\\s+");

        for (String logAttr : logAttrs) {
            if (logAttr.contains(":")) {
                String[] attrKeyValue = logAttr.split(":");
                if (attrKeyValue.length == 2) {

                    String name = attrKeyValue[0];
                    String value = attrKeyValue[1];
                    String unescapedValue = value.replaceAll("\\[COLON\\]", ":").replaceAll("\\[SPACE\\]", " ");

                    log.addLogInfo(name, unescapedValue);
                } else {
                    log.addLogInfo(attrKeyValue[0], "");
                }
            }
        }
        return log;
    }
}
