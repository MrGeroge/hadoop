import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by George on 2017/5/3.
 */
public class TextPartition extends HashPartitioner<Text,Text> {

}
