package sort.test;
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
public class SortMain {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"sort");
        job.setJarByClass(SortMain.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(SortPartitioner.class);
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       // job.setNumReduceTasks(2);
        //job.setSortComparatorClass(TextIntComparator.class);//Map端排序，即对所有键值对进行排序
        //job.setGroupingComparatorClass(TextComparator.class);//Reduce端排序,分组
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
