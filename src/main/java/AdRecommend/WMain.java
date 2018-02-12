package AdRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by George on 2017/5/17.
 */
public class WMain {
    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"W");
        job.setJarByClass(WMain.class);
        job.addCacheFile(new Path("hdfs://localhost:9000/output/part-r-00001").toUri());//N
        job.addCacheFile(new Path("hdfs://localhost:9000/output1/part-r-00000").toUri());//DF
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(WMapper.class);
        job.setReducerClass(WReducer.class);
        //job.setGroupingComparatorClass(WGroup.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output2"));
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
