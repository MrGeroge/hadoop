package HadoopRPC;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * Created by George on 2017/6/7.
 */
public class RPCDemoImpl implements RPCDemo {
    @Override
    public int add(int a, int b) {
        return (a+b);
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
       return RPCDemo.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
