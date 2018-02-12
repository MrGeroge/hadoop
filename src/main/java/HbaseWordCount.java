import MapReduceReadAndWriteHBase.MyTableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by George on 2017/5/24.
 */
public class HbaseWordCount {//HDFS中读数据，统计词频后写入Hbase
    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        //conf.setQuietMode(false); //打印日志信息
       // conf.setBoolean("mapred.output.compress",true);//设置压缩
       // conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);//采用Gzip编码压缩
        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HbaseWordCount.WordCountMap.class);
        //job.setCombinerClass(WordCount.WordCountReduce.class);
        job.setNumReduceTasks(1);
        //job.setPartitionerClass(HashPartitioner.class);
        //job.setReducerClass(WordCount.WordCountReduce.class);
        TableMapReduceUtil.initTableReducerJob(
                "wc",        // output table
                HbaseWordCount.MyTableReducer.class,    // reducer class
                job);

        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        //FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));

        job.waitForCompletion(true);
    }
    public static class WordCountMap extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }

    public  static class MyTableReducer extends TableReducer<Text, IntWritable,
            ImmutableBytesWritable> {
        public static final byte[] CF = "cfi".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
                IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            //System.out.println("count is "+i);
            put.add(CF, COUNT, Bytes.toBytes(i));
            context.write(null, put);
        }
    }
}
