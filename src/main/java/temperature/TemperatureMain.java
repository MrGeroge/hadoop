package temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by George on 2017/5/16.
 */
public class TemperatureMain {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf, "temperature");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(TemperatureMain.class);
        job.setMapOutputKeyClass(TemperaturePair.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(TemperatureMapper.class);
        //job.setCombinerClass(SortReduce.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setSortComparatorClass(TemperatureSort.class);
        job.setGroupingComparatorClass(TemperatureGroup.class);
        job.setNumReduceTasks(3);
        job.setPartitionerClass(TemperaturePartition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
        job.waitForCompletion(true);
    }
}
