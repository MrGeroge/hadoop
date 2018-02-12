package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by George on 2017/5/16.
 */
public class TemperatureMapper extends Mapper<Object,Text,TemperaturePair,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().split("\t");
        TemperaturePair pair=new TemperaturePair();
        //int year=Integer.parseInt(strs[0].split("-")[0]);
        //int temp=Integer.parseInt(strs[2]);
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date= null;
        try {
            date = sdf.parse(strs[0]);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(date);
        pair.setYear(calendar.get(1));
        pair.setTemp(Integer.parseInt(strs[1]));
        context.write(pair,value);
    }
}
