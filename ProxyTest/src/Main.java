import java.lang.reflect.Proxy;
public class Main {

    public static void main(String[] args) {
        Class<?>[] interfaces=new Class[]{ProxyInterfaceTest.class};//动态代理监控的接口
        ProxyInterfaceTestImpl proxyInterfaceTestImpl=new ProxyInterfaceTestImpl("hello world");
        ProxyInvocationHandler proxyInvocationHandler=new ProxyInvocationHandler(proxyInterfaceTestImpl);
        ProxyInterfaceTest proxy=(ProxyInterfaceTest)(Proxy.newProxyInstance(proxyInterfaceTestImpl.getClass().getClassLoader(),interfaces,proxyInvocationHandler));
        String result=proxy.getStatue();
        System.out.println(result);
    }
}
