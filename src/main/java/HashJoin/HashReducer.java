package HashJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dimitra on 10/5/2015.
 */
public class HashReducer extends Reducer<Text,Text,NullWritable,Text> {

    private static final NullWritable nullKey = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException {

        for(Text val: values){
            context.write(nullKey, val);
        }

    }
}
