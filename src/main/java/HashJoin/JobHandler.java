package HashJoin;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by dimitra on 10/5/2015.
 */
public class JobHandler {

    // Partitioning Job
    public Job executePartitioningJob(Configuration conf, String jobName, String nameOfJoinColumn,Integer numReducers, Path inputPath,
                                      Path outputPath, Class mapperClass, Class reducerClass) throws IOException, URISyntaxException {


        FileSystem hdfs = FileSystem.get(conf); // new URI("hdfs://localhost:54310"),
        // delete output path in hdfs if exists
        deleteOutputPathIfExists(hdfs, outputPath);
        System.out.println("work dir is:"+hdfs.getWorkingDirectory());
        // find keyIndex:
        String JoincolumnIndex = extractColumnIndex(inputPath,nameOfJoinColumn,hdfs);

        conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

        conf.set("keyIndex", JoincolumnIndex);
        conf.set("separator", ",");

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());

        job.setNumReduceTasks(numReducers);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }



    // Build - Grace Hash Join Job
    public Job executeGraceHashJoinJob(Configuration conf, String jobName, Integer numberOfMappers, Path pathToLeftPartition,Path pathToRightPartition, Path pathHashJoinOutput,
                                       Class mapperClass, Class reducerClass) throws IOException, URISyntaxException {

        FileSystem hdfs = FileSystem.get(conf);
        // delete output path if exists
        deleteOutputPathIfExists(hdfs, pathHashJoinOutput);
        /*** We set the below configuration because  we want to achieve:
         * goal size=total size of partitioned file in hdfs / mapred.map.tasks ***/                                                   // mapred.min.split.size = 1
        long splitSize = fixSplitSize(conf, pathToRightPartition, numberOfMappers);
        System.out.println("left path from job handler is:"+pathToLeftPartition);
        conf.set("mapred.min.split.size", String.valueOf(splitSize));
        conf.set("mapred.max.split.size", String.valueOf(splitSize));
        conf.set("leftPartitionPath", pathToLeftPartition.toString());

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, pathToRightPartition);
        FileOutputFormat.setOutputPath(job, pathHashJoinOutput);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    // group by job
    public Job executeGroupbyJob(Configuration conf, String jobName, Integer numberOfMappers, Path pathToHashJoin,Path pathToGroupby,
                                 Class mapperClass, Class reducerClass) throws IOException, URISyntaxException {

        FileSystem hdfs = FileSystem.get(conf);
        // delete output path if exists
        deleteOutputPathIfExists(hdfs, pathToGroupby);
        /*** We set the below configuration because  we want to achieve:
         * goal size=total size of hash-join file in hdfs / mapred.map.tasks ***/
        long splitSize = fixSplitSize(conf, pathToHashJoin, numberOfMappers);
        conf.set("mapred.min.split.size", String.valueOf(splitSize));
        conf.set("mapred.max.split.size", String.valueOf(splitSize));
        conf.set("numReduceTasks", String.valueOf(numberOfMappers));
        conf.addResource(new Path("/usr/local/hadoop" + "/conf/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop" + "/conf/hdfs-site.xml"));

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());


        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, pathToHashJoin);
        FileOutputFormat.setOutputPath(job, pathToGroupby);

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }


    /*
    * functions Overview
    *
    * fixSplitSize: goal size=total size of hash-join file in hdfs / mapred.map.tasks
    * extractColumnIndex: extracts attribute for join, from hdfs input files
    * deleteOutputPathIfExists: deletes a path from hdfs
    * */
    private long fixSplitSize(Configuration conf, Path path, int numberOfMappers) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        ContentSummary Summary = fs.getContentSummary(path);
        System.out.println("Split size for [" + path.getName() + "] at " + (Summary.getLength() / numberOfMappers)
                + " for total length [" + Summary.getLength() + "] for requested size ["+numberOfMappers+"]");
        return Summary.getLength() / numberOfMappers;
    }

    
    public String extractColumnIndex(Path inputPath, String nameOfJoinColumn,FileSystem hdfs) throws IOException {
        FSDataInputStream in = hdfs.open(inputPath);
        String columnNames = in.readLine();
        System.out.println("Header: " + columnNames);

        int index = -1;
        String[] column = columnNames.split(",");
        for(int i=0;i<column.length;i++){
            if(column[i].equalsIgnoreCase(nameOfJoinColumn)){
                index = i;
                break;
            }
        }

        System.out.println("column name join:"+column[index]+"\t and it's index for join\t"+index);
        return Integer.toString(index);
    }

    public void deleteOutputPathIfExists(FileSystem hdfs, Path outputPath) throws IOException {
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
    }
}
