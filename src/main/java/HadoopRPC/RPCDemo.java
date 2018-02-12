package HadoopRPC;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Created by George on 2017/6/7.
 */
public interface RPCDemo extends VersionedProtocol {
    public static final long versionID=1L;
    public int add(int a,int b);
}
