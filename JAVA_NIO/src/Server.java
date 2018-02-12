import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.SocketAddress;

public class Server {

    public static void main(String[] args) throws IOException {
        ServerSocket listen = new ServerSocket(); //创建监听套接字
        listen.bind(new InetSocketAddress("127.0.0.1",6666));//绑定监听端口
        Socket socket=listen.accept();
        InputStream inputStream=socket.getInputStream();//读数据
        OutputStream outputStream=socket.getOutputStream();//写数据
        DataInputStream dataInputStream=new DataInputStream(inputStream);
        String str=dataInputStream.readUTF();
        System.out.println("server receive message is"+str);
        DataOutputStream dataOutputStream=new DataOutputStream(outputStream);
        dataOutputStream.writeLong(4096);
        dataOutputStream.flush();
        System.out.println("server send message is"+4096);
        dataInputStream.close();
        dataOutputStream.close();
        inputStream.close();
        outputStream.close();
        socket.close();
        listen.close();
    }
}
