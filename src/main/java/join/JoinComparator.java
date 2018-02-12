package join;

import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/4.
 */
public class JoinComparator extends WritableComparator {
    public JoinComparator() {
        super(JoinPair.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        JoinPair joinPair1=(JoinPair)a;
        JoinPair joinPair2=(JoinPair)b;
        if(joinPair1.getId()==joinPair2.getId()){
            return joinPair1.getProduct().compareTo(joinPair2.getProduct());
        }
        else{
            return (joinPair1.getId()-joinPair2.getId());
        }

    }
}
