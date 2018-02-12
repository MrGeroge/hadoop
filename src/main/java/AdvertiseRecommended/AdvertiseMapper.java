package AdvertiseRecommended;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertiseMapper extends Mapper<Object,Text,AdvertisePair,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().split("\t");
        String ID=strs[0]; //用户ID
        for(int i=1;i<strs.length;i++){
            if(strs[i].isEmpty()){
                return;
            }
            else{
                String[] pairs=strs[i].split(":");
                AdvertisePair pair=new AdvertisePair();
                pair.setKey(pairs[0]);
                pair.setWeight(Integer.parseInt(pairs[1]));
                context.write(pair,new Text(strs[0]));
            }
        }
    }
}
