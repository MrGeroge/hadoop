package IKTest;
import java.io.IOException;
import java.io.StringReader;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
/**
 * Created by George on 2017/5/17.
 */
public class IKMain {
    public static void main(String[] args) throws IOException {
        String text="基于java语言开发的轻量级的中文分词工具包";
        StringReader sr=new StringReader(text);
        IKSegmenter ik=new IKSegmenter(sr, true);
        Lexeme lex=null;
        while((lex=ik.next())!=null){
            System.out.print(lex.getLexemeText()+"|");
        }
    }
}

