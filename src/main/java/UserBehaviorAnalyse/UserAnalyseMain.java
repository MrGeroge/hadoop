package UserBehaviorAnalyse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by George on 2017/5/18.
 */
public class UserAnalyseMain {
    public static void main(String[] args)throws Exception{
        Configuration configuration=new Configuration();
        configuration.set("date","2015-04-26");
        configuration.set("timepoint","9-17-24");
        Job job=Job.getInstance(configuration,"useranalyse");

    }
}
