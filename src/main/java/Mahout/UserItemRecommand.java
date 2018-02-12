package Mahout;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

/**
 * Created by George on 2017/5/31.
 */
public class UserItemRecommand {//基于用户推荐
    public static void main(String[] args)throws Exception{
        DataModel dm = new FileDataModel(new File("/Users/George/Desktop/test.txt"));
        UserSimilarity us=new PearsonCorrelationSimilarity(dm);//基于用户相似
        UserNeighborhood unb= new NearestNUserNeighborhood(3,us,dm);
        Recommender re=new GenericUserBasedRecommender(dm,unb,us);
        List<RecommendedItem> list=re.recommend(1,2);//给用户1推荐2个Item
        for(RecommendedItem recommendedItem:list){
            System.out.println(recommendedItem);
        }
    }
}
