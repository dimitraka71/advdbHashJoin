package HashJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dimitra on 10/5/2015.
 */
public class GroupbyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        context.write(value,one);
    }

}
