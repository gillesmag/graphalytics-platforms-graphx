package nl.tudelft.granula.modeller.platform;

/**
 * Created by wlngai on 10/10/16.
 */

import nl.tudelft.granula.modeller.Type;
import nl.tudelft.granula.modeller.job.Job;
import nl.tudelft.granula.modeller.job.Overview;
import nl.tudelft.granula.modeller.platform.info.BasicInfo;
import nl.tudelft.granula.modeller.platform.info.Source;
import nl.tudelft.granula.modeller.platform.operation.*;
import nl.tudelft.granula.modeller.rule.derivation.DerivationRule;
import nl.tudelft.granula.modeller.rule.extraction.GraphXExtractionRule;
import nl.tudelft.granula.modeller.rule.filling.UniqueOperationFilling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphX extends PlatformModel {

    public GraphX() {
        super();
        addOperationModel(new LoadGraph());
        addOperationModel(new OffloadGraph());
        addOperationModel(new ProcessGraph());
        addOperationModel(new SchedulerStage());
        addOperationModel(new SchedulerProcessStage());
        addOperationModel(new SparkAppSparkJob());
        addOperationModel(new TopActorTopMission());
    }

    public void loadRules() {

        addFillingRule(new UniqueOperationFilling(2, Type.TopActor, Type.TopMission));

        addInfoDerivation(new JobNameDerivationRule(4));
        addInfoDerivation(new JobInfoRule(20));
        addExtraction(new GraphXExtractionRule(1));

    }


    protected class JobInfoRule extends DerivationRule {

        public JobInfoRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
            platform.setName("A GraphX job");
            platform.setType("GraphX");

            Job job = platform.getJob();
            Overview overview = job.getOverview();

            overview.setDescription("This is a GraphX job.");

            Operation jobOper = platform.findOperation(Type.SparkApp, Type.SparkJob);
            jobOper.parentId = null;
            platform.addRoot(jobOper.getUuid());

            try {
                Operation processGraph = platform.findOperation(Type.GraphX, Type.ProcessGraph);
                long processingTime = Long.parseLong(processGraph.getInfo("Duration").getValue());

                Operation loadGraph = platform.findOperation(Type.GraphX, Type.LoadGraph);
                long loadTime = Long.parseLong(loadGraph.getInfo("Duration").getValue());

                Operation topOperation = platform.findOperation(Type.TopActor, Type.TopMission);
                long totalTime = Long.parseLong(topOperation.getInfo("Duration").getValue());

                long otherTime = totalTime - loadTime - processingTime;

                Map<String, Long> breakDown = new HashMap<>();
                breakDown.put("ProcessingTime", processingTime);
                breakDown.put("IOTime", loadTime);
                breakDown.put("Overhead", otherTime);
                overview.setBreakDown(breakDown);
            } catch (Exception e) {
                System.out.println(String.format("JobInfoRule encounter %s exception when calculating breakdown.", e.toString()));
            }


            return true;


        }
    }


    protected class JobNameDerivationRule extends DerivationRule {

        public JobNameDerivationRule(int level) {
            super(level);
        }

        @Override
        public boolean execute() {

            Platform platform = (Platform) entity;
//
//
//            BasicInfo jobNameInfo = new BasicInfo("JobName");
//            jobNameInfo.addInfo("unspecified", new ArrayList<Source>());
//            job.addInfo(jobNameInfo);
//
//            String jobName = null;
//            List<Operation> operations = job.findOperations(OpenGType.MRApp, OpenGType.MRJob);
//            for (Operation operation : operations) {
//                jobName = operation.getInfo("JobName").getValue();
//            }
//            if(jobName == null) {
//                throw new IllegalStateException();
//            }

            platform.setName("A GraphX job");
            platform.setType("GraphX");

            return true;

        }
    }


    public BasicInfo basicInfo(String key, String value) {
        List<Source> sources = new ArrayList<>();
        BasicInfo info = new BasicInfo(key);
        info.setDescription("No description was set.");
        info.addInfo(value, sources);
        return info;
    }

}