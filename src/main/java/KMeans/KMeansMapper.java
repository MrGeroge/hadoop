package KMeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by George on 2017/6/5.
 */
public class KMeansMapper extends Mapper<Object,Text,KMeansNode,KMeansNode> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //ArrayWritable kMeansNodes=new ArrayWritable(KMeansNode.class);
        String[] nodes=value.toString().split("\\|");
        KMeansNode[] kMeansNodes=new KMeansNode[nodes.length];
        for(int i=0;i<nodes.length;i++) {
            String[] location = nodes[i].split(":");
            System.out.println("nodes[i]="+nodes[i]);
            KMeansNode kMeansNode = new KMeansNode();
            kMeansNode.setX(Integer.parseInt(location[0]));
            System.out.println("location[0]="+location[0]);
            kMeansNode.setY(Integer.parseInt(location[1]));
            kMeansNodes[i]=kMeansNode;
        }
        Random random=new Random();
        int center1=random.nextInt(kMeansNodes.length-1);
        int center2;
        int center3;
        while((center2=random.nextInt(kMeansNodes.length-1))==center1){

        }
        while((center3=random.nextInt(kMeansNodes.length-1))==center1||(center3==center2)){

        }
        System.out.println("center1="+center1);
        System.out.println("center2="+center2);
        System.out.println("center3="+center3);
        KMeansNode kMeansCenterNode1=kMeansNodes[center1];
        KMeansNode kMeansCenterNode2=kMeansNodes[center2];
        KMeansNode kMeansCenterNode3=kMeansNodes[center3];
        //Iterator<KMeansNode> iterator=kMeansNodes.iterator();
        for(int i=0;i<kMeansNodes.length;i++){
            KMeansNode node=kMeansNodes[i];
            double distance1=Math.sqrt(Math.pow(node.getX() - kMeansCenterNode1.getX(),2)+Math.pow(node.getY() - kMeansCenterNode1.getY(),2));
            double distance2=Math.sqrt(Math.pow(node.getX() - kMeansCenterNode2.getX(),2)+Math.pow(node.getY() - kMeansCenterNode2.getY(),2));
            double distance3=Math.sqrt(Math.pow(node.getX() - kMeansCenterNode3.getX(),2)+Math.pow(node.getY() - kMeansCenterNode3.getY(),2));
            double min;
            int minlocationt;
            if(distance1<distance2){
                min=distance1;
                minlocationt=1;
            }
            else{
                min=distance2;
                minlocationt=2;
            }
            if(min<distance3){
                min=distance3;
                minlocationt=3;
            }
            if (minlocationt == 1) {//分簇
                context.write(kMeansCenterNode1,node);
            }
            else if(minlocationt==2){
                context.write(kMeansCenterNode2,node);
            }
            else{
                context.write(kMeansCenterNode3,node);
            }
        }
    }
    }
