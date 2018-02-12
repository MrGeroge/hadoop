package KMeans;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/6/5.
 */
public class KMeansGroup extends WritableComparator {
    public  KMeansGroup(){
        super(KMeansNode.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KMeansNode node1=(KMeansNode)a;
        KMeansNode node2=(KMeansNode)b;
        if((node1.getX()==node2.getX())&&(node1.getY()==node2.getY())){
            return 0;
        }
        else{
            return -1;
        }
    }
}
