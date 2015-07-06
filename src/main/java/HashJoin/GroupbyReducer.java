package HashJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by dimitra on 10/5/2015.
 */
public class GroupbyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {


    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int cnt = 0;
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            cnt += it.next().get();
        }
        context.write(key, new IntWritable(cnt));
    }
}
