package HashJoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by dimitra on 10/5/2015.
 */
public class GraceHashJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = Logger.getLogger(GraceHashJoinMapper.class.getName());

    private Text fixedKey = new Text();
    //private Map<String, List<String>> leftPartitionMap = new HashMap<String, List<String>>();
    private Text outputValueText = new Text();
    private String rightPartName;
    private String leftPartitionPath;
    private FSDataInputStream in;
    private String previousKey = null;

    private BufferedInputStream reader;
    private DataInputStream dis;


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // get left path
        leftPartitionPath = context.getConfiguration().get("leftPartitionPath");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String leftPartName = fileSplit.getPath().getName();
        System.out.println("left File name being processed: " + leftPartName);
        System.out.println(" left Corresponding bucket at: " + leftPartitionPath + "/" + leftPartName);
        fixedKey.set("ok");

        FileSystem hdfs = FileSystem.get(context.getConfiguration());

        in = hdfs.open(new Path(leftPartitionPath + "/" + leftPartName));
        //in.seek(0);
        reader = new BufferedInputStream(in,67108864 / 64); // read 64 MB block
        dis = new DataInputStream(reader);

        // filename of S_dir (right)
        FileSplit inputSplit=(FileSplit)context.getInputSplit();
        rightPartName=inputSplit.getPath().getName();
        System.out.println("filename of S_dir logically:\t"+rightPartName+"\t from path\t"+inputSplit.getPath()); // S_dir

    }


    // read S tuples checked by part file name OF BIGGER FILE
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String[] currentKey =  value.toString().split(",");

        if(currentKey[0].equals(previousKey)){
            dis.reset();
        }else{
            dis.mark(1000000000);
        }

        String[] tuple = getNextLeftTuple();
        while(tuple !=null ){
            if(tuple[0].equals(currentKey[0])) {
                // do join
                outputValueText.set(value.toString() + "," + tuple[1]);
                context.write(fixedKey, outputValueText);
            }
            dis.mark(1000);
            tuple = getNextLeftTuple();
        }
        dis.reset();
        previousKey = currentKey[0];

    }


    private String[] getNextLeftTuple() throws IOException {
        String line = dis.readLine();
        if (line == null) {
            return null;
        }

        String[] l = line.split(",");
        String leftKey = l[0];
        String restOfLeftLine = line.substring(l[0].length(), line.length());

        return new String[]{leftKey, restOfLeftLine.substring(1)};
    }
}
