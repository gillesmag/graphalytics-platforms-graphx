package science.atlarge.granula.modeller;

import science.atlarge.granula.archiver.GranulaExecutor;
import science.atlarge.granula.modeller.entity.Execution;
import science.atlarge.granula.modeller.job.JobModel;
import science.atlarge.granula.util.FileUtil;
import science.atlarge.granula.util.json.JsonUtil;
import science.atlarge.graphalytics.granula.GranulaPlugin;

import java.nio.file.Paths;

/**
 * Created by wing on 21-8-15.
 */
public class ModelTester {
    public static void main(String[] args) {
        String inputPath = "/media/sf_Large/Large/runner/result/archive2/r512288-graphx/report/report-graphx-161109-164131/log/r466181_example-directed_BFS";
        String outputPath = "/media/sf_Large/Large/runner/result/archive2/r512288-graphx/output";


        Execution execution = (Execution) JsonUtil.fromJson(FileUtil.readFile(Paths.get(inputPath).resolve("execution").resolve("execution-log.js")), Execution.class);
        execution.setLogPath(inputPath);
        execution.setEndTime(System.currentTimeMillis());
        execution.setArcPath(outputPath);
        JobModel jobModel = new JobModel(GranulaPlugin.getPlatformModel(execution.getPlatform()));

        GranulaExecutor granulaExecutor = new GranulaExecutor();
        granulaExecutor.setEnvEnabled(false);
        granulaExecutor.setExecution(execution);
        granulaExecutor.buildJobArchive(jobModel);

    }
}
