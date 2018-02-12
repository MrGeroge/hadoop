/**
 * Created by George on 2017/3/7.
 */
public class ProxyInterfaceTestImpl implements ProxyInterfaceTest{ //目标类
    private String msg;
    public ProxyInterfaceTestImpl(String msg){
        this.msg=msg;
    }
    public String getStatue() {
        return this.msg;
    }
}
