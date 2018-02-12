package AdvertiseRecommended;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sort.*;

import java.io.IOException;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertiseMain {//广告推荐
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf, "advertise");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(AdvertiseMain.class);
        job.setMapOutputKeyClass(AdvertisePair.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(AdvertiseMapper.class);
        //job.setCombinerClass(SortReduce.class);
        job.setReducerClass(AdvertiseReducer.class);
        job.setSortComparatorClass(AdvertiseSort.class);
        job.setGroupingComparatorClass(AdvertiseCompare.class);
        job.setNumReduceTasks(3);
        job.setPartitionerClass(AdvertisePartition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
        job.waitForCompletion(true);
    }
}
