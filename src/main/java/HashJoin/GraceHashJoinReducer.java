package HashJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by dimitra on 10/5/2015.
 */
public class GraceHashJoinReducer extends Reducer<Text,Text,NullWritable,Text> {

    private static final NullWritable nullKey = NullWritable.get();
    private Text outputValueText = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException {

        Iterator<Text> it = values.iterator();
        while(it.hasNext()){
            outputValueText.set(it.next());
            context.write(null, outputValueText);
        }

    }
}
