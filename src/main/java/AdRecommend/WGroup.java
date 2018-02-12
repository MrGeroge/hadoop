package AdRecommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/17.
 */
public class WGroup extends WritableComparator {
    public WGroup(){
        super(Text.class);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text text1=(Text)a;
        Text text2=(Text)b;
        if(text1.toString().equals(text2.toString())){
            return 0;
        }
        else{
            return text1.toString().compareTo(text2.toString());
        }
    }
}
