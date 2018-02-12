package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class SortMapper extends Mapper<Object,Text,StringIntPair,IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit=(FileSplit)context.getInputSplit();
        System.out.println(fileSplit.getPath().getName());
        StringIntPair pair=new StringIntPair();
        pair.setKey(key.toString());
        int intValue=Integer.parseInt(value.toString());
        pair.setValue(intValue);
        context.write(pair,new IntWritable(intValue));
    }
}
