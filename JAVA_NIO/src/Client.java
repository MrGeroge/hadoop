import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by George on 2017/3/7.
 */
public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket=new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1",6666));
        InputStream inputStream=socket.getInputStream();
        OutputStream outputStream=socket.getOutputStream();
        DataInputStream dataInputStream=new DataInputStream(inputStream);
        DataOutputStream dataOutputStream=new DataOutputStream(outputStream);
        dataOutputStream.writeUTF("hi  server");
        dataOutputStream.flush();
        System.out.println("client is saying hi server");
        Long msg=dataInputStream.readLong();
        System.out.println("client receive msg is"+msg);
        dataOutputStream.close();
        dataInputStream.close();
        outputStream.close();
        inputStream.close();
        socket.close();
    }
}
