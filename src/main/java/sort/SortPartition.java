package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by George on 2017/5/4.
 */
public class SortPartition extends Partitioner<StringIntPair,IntWritable>{

    @Override
    public int getPartition(StringIntPair stringIntPair, IntWritable intWritable, int i) {
       return (stringIntPair.getKey().hashCode() & Integer.MAX_VALUE)%i;
    }
}
