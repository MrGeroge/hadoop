package UserBehaviorAnalyse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.TreeMap;


/**
 * Created by George on 2017/5/18.
 */
public class UserAnalyseReducer extends Reducer<Text,Text,NullWritable,Text> {
    private String date;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.date=context.getConfiguration().get("date");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       String imsi=key.toString().split("\\|")[0];//用户
        String timeFlag=key.toString().split("\\|")[1];//时间段
        TreeMap<Long,String> uploads=new TreeMap<Long,String>();
        for(Text t:values){
            String position=t.toString().split("\\|")[0];
            Long time=Long.parseLong(t.toString().split("\\|")[1]);//时间s
            uploads.put(time,position);
        }
        Map.Entry<Long,String> pair,nextpair;
        HashMap<String,Float> locs=new HashMap<String,Float>();//保存每个地点停留的时间
        Iterator<Map.Entry<Long,String>> it=uploads.entrySet().iterator();
        pair=it.next();
        while(it.hasNext()){
            nextpair=it.next();
            float diff=(float)(nextpair.getKey()-pair.getKey())/60.0f;//两条记录相差多少分钟
            if(diff<=60){
                if(locs.containsKey(pair.getValue())){
                    locs.put(pair.getValue(),locs.get(pair.getValue())+diff);
                }
                else{
                    locs.put(pair.getValue(),diff);
                }
            }
            pair=nextpair;
        }
        Iterator<Map.Entry<String,Float>> its=locs.entrySet().iterator();
        while(its.hasNext()){
            Map.Entry<String,Float> loc=its.next();
            StringBuilder sb=new StringBuilder();
            sb.append(imsi).append("|");
            sb.append(timeFlag).append("|");
            sb.append(loc.getKey()).append("|");
            sb.append(loc.getValue());
            context.write(NullWritable.get(),new Text(sb.toString()));
        }
    }
}
