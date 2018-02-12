package AdRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by George on 2017/5/17.
 */
public class DFMain {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"nf");
        job.setJarByClass(DFMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(DFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(DFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/output"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output1"));
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
