package sort;


import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/4.
 */
public class SortComparator extends WritableComparator{
    public SortComparator() {
        super(StringIntPair.class,true);  //注册StringIntPair
    }

    @Override
    public int compare(Object a, Object b) {
        StringIntPair pair1=(StringIntPair)a;
        StringIntPair pair2=(StringIntPair)b;
        if(pair1.getKey().equals(pair2.getKey())){
            System.out.println("exe there");
            System.out.println("###################");
            return (pair1.getValue()-pair2.getValue());
        }
        else{
            System.out.println("exe here");
            System.out.println("###################");
            return pair1.getKey().compareTo(pair2.getKey());
        }
    }
}
