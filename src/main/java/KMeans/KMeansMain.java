package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by George on 2017/6/5.
 */
public class KMeansMain {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kmeans");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(KMeansMain.class);
        job.setMapOutputKeyClass(KMeansNode.class);
        job.setMapOutputValueClass(KMeansNode.class);
        job.setMapperClass(KMeansMapper.class);
        //job.setCombinerClass(SortReduce.class);
        job.setReducerClass(KMeansReducer.class);
        //job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(KMeansGroup.class);
        //job.setNumReduceTasks(2);
        //job.setPartitionerClass(SortPartition.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/kmeans/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/kmeans/output"));
        job.waitForCompletion(true);
    }
}
