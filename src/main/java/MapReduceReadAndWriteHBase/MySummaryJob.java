package MapReduceReadAndWriteHBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by George on 2017/5/24.
 */
public class MySummaryJob {
    public static void main(String[] args)throws Exception{
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"ExampleSummary");
        job.setJarByClass(MySummaryJob.class);     // class that contains mapper and reducer
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs
        TableMapReduceUtil.initTableMapperJob(
                "test",
                scan,
                MyMapper.class,
                Text.class,      //mapper output key
                IntWritable.class,  // mapper output value
                job);
// input table
// Scan instance to control CF and attribute selection
// mapper class
// mapper output key
        TableMapReduceUtil.initTableReducerJob(
                "test",        // output table
                MyTableReducer.class,    // reducer class
                job);
        job.setNumReduceTasks(1);   // at least one, adjust as required
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

    }
}
