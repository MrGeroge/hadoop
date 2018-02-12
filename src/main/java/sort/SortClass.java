package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class SortClass {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf, "sort");
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(SortClass.class);
        job.setMapOutputKeyClass(StringIntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(SortMapper.class);
        //job.setCombinerClass(SortReduce.class);
        job.setReducerClass(SortReduce.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(TextComparator.class);
        job.setNumReduceTasks(1);
        job.setPartitionerClass(SortPartition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
        job.waitForCompletion(true);
    }
}
