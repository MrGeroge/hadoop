package Mahout;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

/**
 * Created by George on 2017/5/31.
 */
public class ItemRecommand {//基于商品推荐
    public static void main(String[] args)throws Exception{
        DataModel dm = new FileDataModel(new File("/Users/George/Desktop/test.txt"));
        ItemSimilarity us=new PearsonCorrelationSimilarity(dm);
        Recommender re=new GenericItemBasedRecommender(dm,us);
        List<RecommendedItem> list=re.recommend(1,2);
        for(RecommendedItem recommendedItem:list){
            System.out.println(recommendedItem);
        }
    }
}
