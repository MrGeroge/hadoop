import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Created by George on 2017/3/7.
 */
public class ProxyInvocationHandler implements InvocationHandler{
    private ProxyInterfaceTestImpl proxyInterfaceTestImpl;  //目标对象
    public ProxyInvocationHandler(ProxyInterfaceTestImpl proxyInterfaceTestImpl){
        this.proxyInterfaceTestImpl=proxyInterfaceTestImpl;
    }

    public String invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("proxy invoke distribute start");
        String message=(String)method.invoke(proxyInterfaceTestImpl);
        System.out.println("proxy invoke distribute end");
        return message;
    }
}
