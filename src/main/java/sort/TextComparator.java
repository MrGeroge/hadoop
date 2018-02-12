package sort;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/4.
 */
public class TextComparator extends WritableComparator {
    public TextComparator() {
        super(StringIntPair.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        StringIntPair pair1=(StringIntPair)a;
        StringIntPair pair2=(StringIntPair)b;
        return pair1.getKey().compareTo(pair2.getKey());
    }
}
