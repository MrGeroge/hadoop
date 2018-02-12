package HadoopRPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * Created by George on 2017/6/7.
 */
public class RPCClient {
    public static void main(String[] args)throws Exception{
        RPCDemo proxy=(RPCDemo) RPC.getProxy(RPCDemo.class,1L,new InetSocketAddress("127.0.0.1",14000),new Configuration());
        int result=proxy.add(1,2);
        System.out.print(result);
    }
}
