package sort.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/4.
 */
public class TextComparator extends WritableComparator {
    public TextComparator(){
        super(TextInt.class, true);
    }

    @Override
    @SuppressWarnings("all")

    public int compare(WritableComparable a, WritableComparable b) {
        TextInt o1 = (TextInt) a;
        TextInt o2 = (TextInt) b;
        return o1.getStr().compareTo(o2.getStr());
    }
}