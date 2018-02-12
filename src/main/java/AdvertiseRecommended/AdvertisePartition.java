package AdvertiseRecommended;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import sort.StringIntPair;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertisePartition extends Partitioner<AdvertisePair,Text> {

    @Override
    public int getPartition(AdvertisePair advertisePair, Text text, int i) {//对key-value进行分类，把关键字相同的key-value给同一个Reduce处理
        return (advertisePair.getKey().hashCode() & Integer.MAX_VALUE)%i;
    }
}
