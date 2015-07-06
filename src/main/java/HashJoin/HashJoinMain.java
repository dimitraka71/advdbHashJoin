package HashJoin;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

/**
 * Created by dimitra on 10/5/2015.
 */
public class HashJoinMain extends Configured implements Tool {

    private JobHandler jobHandler = new JobHandler();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HashJoinMain(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{

        Path pathToLeftTable = new Path(args[0]);
        Path pathToRightTable = new Path(args[1]);

        Path pathToLeftPartition = new Path("/user/advdb06/partitionedinput/R_dir/");
        Path pathToRightPartition = new Path("/user/advdb06/partitionedinput/S_dir/");
        Path pathHashJoinOutput = new Path("/user/advdb06/hashjoinoutput/");
        Path pathGroupByOutput = new Path("/user/advdb06/groupbyoutput/");

        String nameOfJoinColumn = args[2];

        Integer numReduceTasks =  Integer.parseInt(args[3])/2;


        // Partitioning Phase
        Job leftPartitionJob = jobHandler.executePartitioningJob(getConf(), "leftPartition", nameOfJoinColumn, numReduceTasks, pathToLeftTable,
                pathToLeftPartition, HashMapper.class, HashReducer.class);
        leftPartitionJob.waitForCompletion(true);

        Job rightPartitionJob = jobHandler.executePartitioningJob(getConf(), "rightPartition", nameOfJoinColumn, numReduceTasks, pathToRightTable,
                pathToRightPartition, HashMapper.class, HashReducer.class);

        rightPartitionJob.waitForCompletion(true);

        // Build Phase
        Job graceHashJoinJob = jobHandler.executeGraceHashJoinJob(getConf(), "hashJoin", numReduceTasks, pathToLeftPartition, pathToRightPartition, pathHashJoinOutput,
                GraceHashJoinMapper.class, GraceHashJoinReducer.class);
        graceHashJoinJob.waitForCompletion(true);
        System.out.println("Job\t"+graceHashJoinJob.getJobName()+"\t completed succesfully!");
        // Group by Job
        Job groupbyJob = jobHandler.executeGroupbyJob(getConf(), "groupBy", numReduceTasks, pathHashJoinOutput, pathGroupByOutput,
                GroupbyMapper.class, GroupbyReducer.class);
        groupbyJob.waitForCompletion(true);
        System.out.println("Job\t"+groupbyJob.getJobName()+"\t completed succesfully!");

        return 1;
    }

}
