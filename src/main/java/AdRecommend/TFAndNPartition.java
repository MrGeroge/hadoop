package AdRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by George on 2017/5/17.
 */
public class TFAndNPartition extends Partitioner<Text,IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable integer, int i) {
        if(text.toString().equals("count")){
            return 1;
        }
        else{
            return 0;
        }
    }
}
