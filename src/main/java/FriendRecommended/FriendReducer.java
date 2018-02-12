package FriendRecommended;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by George on 2017/5/11.
 */
public class FriendReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set=new HashSet<String>();
        for(Text t:values){
            set.add(t.toString());
        }
        if(set.size()>1){
            for(Iterator i=set.iterator();i.hasNext();){
                String value1=i.next().toString();
                for(Iterator k=set.iterator();k.hasNext();){
                    String value2=k.next().toString();
                    if(!value1.equals(value2)) {
                        context.write(new Text(value1), new Text(value2));
                    }
                }
            }
        }
    }
}
