package HiveDemo;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by George on 2017/5/24.
 */
public class UDFDemo extends UDF { //自定义UDF,大写单词
    public Text evaluate(final Text s){
        return new Text(s.toString().toUpperCase());
    }
}
