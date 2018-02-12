package AdRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by George on 2017/5/17.
 */
public class TFAndNMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().split("\t");
        String ID=strs[0];
        String content=strs[1];
        StringReader sr=new StringReader(content);
        IKSegmenter ikSegmenter=new IKSegmenter(sr,true);
        Lexeme lexeme=null;
        while((lexeme=ikSegmenter.next())!=null){//得到每个分词
            String keyword=lexeme.getLexemeText()+"_"+ID;
            context.write(new Text(keyword),new IntWritable(1));
        }
        context.write(new Text("count"),new IntWritable(1));
    }
}

