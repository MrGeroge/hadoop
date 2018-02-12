package org.george.mahout;

/**
 * Created by George on 2017/5/21.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class WikiDriver {
    //hadoop的安装目录
   // static final String HADOOP_HOME = "/usr/local/Cellar/hadoop/2.7.3";
    //HDFS的临时目录，用来放job1的输出结果
   // static final String TMP_PATH = "/usr/tmp/part-r-00000";

    //操作hdfs的文件夹
    /*static void OperatingFiles(String input, String output) {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        Path tmpPath = new Path(TMP_PATH);
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop/hdfs-site.xml"));
        try {
            FileSystem hdfs = FileSystem.get(conf);
            if (!hdfs.exists(inputPath)) {
                System.out.println("input path no exist!");
            }
            if (hdfs.exists(outputPath)) {
                System.out.println("output path:"+outputPath.toString()+ " exist, deleting this path...");
                hdfs.delete(outputPath,true);
            }
            if (hdfs.exists(tmpPath)) {
                System.out.println("tmp path:"+tmpPath.toString()+ " exist, deleting this path...");
                hdfs.delete(tmpPath,true);
            }
        } catch (Exception e) {

        }

    }*/
    public static void main(String[] args) throws IOException {
        //OperatingFiles(args[0], args[1]);//参数一和参数二分别表示输入输出路径
        Configuration job1Conf = new Configuration();
        Job job1 = new Job(job1Conf, "job1");
        job1.setJarByClass(WikiDriver.class);
        job1.setMapperClass(WikipediaToItemPrefsMapper.class);
        job1.setReducerClass(WikipediaToUserVectorReducer.class);
        job1.setMapOutputKeyClass(VarLongWritable.class);
        job1.setMapOutputValueClass(VarLongWritable.class);

        //将job1输出的文件格式设置为SequenceFileOutputFormat
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(VarLongWritable.class);
        job1.setOutputValueClass(VectorWritable.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/usr/input"));
        FileOutputFormat.setOutputPath(job1, new Path("hdfs://localhost:9000/usr/tmp"));//Job1的输出结果保存在tmp

        Configuration job2Conf = new Configuration();
        Job job2 = new Job(job2Conf, "job2");
        job2.setJarByClass(WikiDriver.class);
        job2.setMapperClass(UserVectorToCooccurrenceMapper.class);
        job2.setReducerClass(UserVectorToCooccurrenceReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(VectorWritable.class);

        //将job2的输入文件格式设置为SequenceFileInputFormat
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/usr/tmp"));
        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:9000/usr/output"));//参数二表示Job2的输出结果

        ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());
        ctrlJob1.setJob(job1);

        ControlledJob ctrlJob2 = new ControlledJob(job2.getConfiguration());
        ctrlJob2.setJob(job2);

        ctrlJob2.addDependingJob(ctrlJob1);

        JobControl JC = new JobControl("wiki job");
        JC.addJob(ctrlJob1);
        JC.addJob(ctrlJob2);//先执行Job1，再执行Job2

        Thread thread = new Thread(JC);
        thread.start();
        while (true) {
            if(JC.allFinished()) {
                System.out.println(JC.getSuccessfulJobList());
                JC.stop();
                System.exit(0);
            }

            if (JC.getFailedJobList().size() > 0) {
                System.out.println(JC.getFailedJobList());
                JC.stop();
                System.exit(1);
            }
        }
    }
}
