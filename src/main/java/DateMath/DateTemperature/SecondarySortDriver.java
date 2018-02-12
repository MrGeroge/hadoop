package DateMath.DateTemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/** 
 * SecondarySortDriver is driver class for submitting secondary sort job to Hadoop.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortDriver{

	private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(SecondarySortDriver.class);
		job.setJobName("SecondarySortDriver");

		// args[0] = input directory
		// args[1] = output directory
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output"));

		job.setOutputKeyClass(DateTemperaturePair.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(DateTemperatureComparator.class);//设置组合键的比较策略，溢写到分区文件前排序
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setPartitionerClass(DateTemperaturePartitioner.class);
		job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

		boolean status = job.waitForCompletion(true);



	}



}



