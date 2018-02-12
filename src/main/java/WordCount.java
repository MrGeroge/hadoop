import java.io.IOException;
import java.util.StringTokenizer;

import io.netty.handler.codec.compression.CompressionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCount {

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

    public static class WordCountReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.setQuietMode(false); //打印日志信息
        conf.setBoolean("mapred.output.compress",true);//设置压缩
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);//采用Gzip编码压缩
        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setNumReduceTasks(3);
        job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(WordCountReduce.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));

        job.waitForCompletion(true);
    }
}
