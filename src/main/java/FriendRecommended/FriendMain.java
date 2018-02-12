package FriendRecommended;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by George on 2017/5/11.
 */
public class FriendMain {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"join");
        job.setJarByClass(FriendMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FriendReducer.class);
        //job.setSortComparatorClass(JoinComparator.class);//Map端排序
        //job.setGroupingComparatorClass(JoinComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/qq/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/qq/output"));
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
