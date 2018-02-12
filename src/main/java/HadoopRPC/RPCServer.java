package HadoopRPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by George on 2017/6/7.
 */
public class RPCServer {
    public static void main(String[] args) throws Exception {
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("127.0.0.1").setPort(14000)
                .setProtocol(RPCDemo.class)
                .setInstance(new RPCDemoImpl()).build();
        server.start();
    }
}
