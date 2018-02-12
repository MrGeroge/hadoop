package UserBehaviorAnalyse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by George on 2017/5/18.
 */
public class UserAnalyseMapper extends Mapper<Object,Text,Text,Text> {
    private String date; //当天时间
    private String[] timepoint;//时间段
    private boolean dataSource;//数据来源pos or net
    private String imsi;//用户
    private String position;//位置
    private String time;//时间
    private Date datetime;
    private String timeFlag;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.date=context.getConfiguration().get("date");
        this.timepoint=context.getConfiguration().get("timepoint").split("-");
        FileSplit split=(FileSplit)context.getInputSplit();
        if(split.getPath().getName().contains("pos")){
            this.dataSource=true;
        }
        else{
            this.dataSource=false;
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] linesplit=line.split("\t");
        if(dataSource){
            this.imsi=linesplit[1];
            this.position=linesplit[2];
            this.time=linesplit[3];
        }
        else{
            this.imsi=linesplit[0];
            this.position=linesplit[2];
            this.time=linesplit[3];
        }
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            this.datetime=sdf.parse(this.time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int i=0,n=timepoint.length;
        int hour=Integer.valueOf(this.time.split(" ")[1].split(":")[0]);
        while(i<n&&Integer.valueOf(timepoint[i])<hour){
            i++;
        }
        if(i<n){
            if(i==0){
                this.timeFlag="0-"+timepoint[i];
            }
            else{
                this.timeFlag=timepoint[i-1]+"-"+timepoint[i];
            }
        }
        long t=datetime.getTime()/1000L;
        context.write(new Text(this.imsi+"|"+this.timeFlag),new Text(position+"|"+String.valueOf(t)));
    }
}
