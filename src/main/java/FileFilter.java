import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by George on 2017/5/3.
 */
public class FileFilter implements PathFilter {//过滤文件
    @Override
    public boolean accept(Path path) {//对path进行正则校验
        return false;
    }
}
