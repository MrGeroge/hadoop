package sort.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by George on 2017/5/4.
 */
public class SortPartitioner extends Partitioner<TextInt, IntWritable>{

    @Override
    public int getPartition(TextInt textInt, IntWritable value, int numReducers) {
        return textInt.getStr().hashCode() & Integer.MAX_VALUE % numReducers;
    }
}
