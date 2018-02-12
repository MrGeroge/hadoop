package Mahout;

import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.EstimatedPreferenceCapper;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by George on 2017/5/31.
 */
public class UserRecommendTest {//基于用户的协同过滤推荐算法测试
    private DataModel model;
    private UserNeighborhood neighborhood;
    private UserSimilarity userSimilarity;
    private ItemSimilarity itemSimilarity;
    private EstimatedPreferenceCapper capper;

    @Before
    public void init() throws Exception {
        RandomUtils.useTestSeed();//只用于结果可重复的例子
        model = new FileDataModel(new File("/Users/George/Desktop/item.csv"));
        userSimilarity = new EuclideanDistanceSimilarity(model);
        ;//基于用户相似
        itemSimilarity = new PearsonCorrelationSimilarity(model);//基于商品相似
        neighborhood = new NearestNUserNeighborhood(3, userSimilarity, model);
        if (Float.isNaN(model.getMinPreference()) && Float.isNaN(model.getMaxPreference())) {
            capper = null;
        } else {
            capper = new EstimatedPreferenceCapper(model);
        }

    }

    public float estimateUserPreference(long userID, long itemID) throws Exception {//基于用户推荐的实现
        Float actualPref = model.getPreferenceValue(userID, itemID);
        if (actualPref != null) {
            return actualPref;
        }
        long[] theNeighborhood = neighborhood.getUserNeighborhood(userID);//返回与用户userID相似的其他用户
        return doUserEstimatePreference(userID, theNeighborhood, itemID);
    }

    protected float doUserEstimatePreference(long theUserID, long[] theNeighborhood, long itemID) throws Exception {
        if (theNeighborhood.length == 0) {
            return Float.NaN;
        }
        double preference = 0.0;
        double totalSimilarity = 0.0;
        int count = 0;
        for (long userID : theNeighborhood) {
            if (userID != theUserID) {//说明是其他用户
                // See GenericItemBasedRecommender.doEstimatePreference() too
                Float pref = model.getPreferenceValue(userID, itemID);
                if (pref != null) {//其他用户userID已经对商品itemID打分了
                    double theSimilarity = userSimilarity.userSimilarity(theUserID, userID);//其他用户userID与当前用户theUserID的相似度
                    if (!Double.isNaN(theSimilarity)) {
                        preference += theSimilarity * pref;//preference为当前用户theUserID对商品itemID的打分
                        totalSimilarity += theSimilarity;
                        count++;
                    }
                }
            }
        }
        // Throw out the estimate if it was based on no data points, of course, but also if based on
        // just one. This is a bit of a band-aid on the 'stock' item-based algorithm for the moment.
        // The reason is that in this case the estimate is, simply, the user's rating for one item
        // that happened to have a defined similarity. The similarity score doesn't matter, and that
        // seems like a bad situation.
        if (count <= 1) {
            return Float.NaN;
        }
        float estimate = (float) (preference / totalSimilarity);
        if (capper != null) {
            estimate = capper.capEstimate(estimate);//estimate为用户userID对物品itemID的预测分
        }
        return estimate;
    }

    @Test
    public void testUserCF() throws Exception {
        LongPrimitiveIterator iter = model.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            Recommender recommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
            List<RecommendedItem> list = recommender.recommend(uid, 2);
            for (RecommendedItem recommendedItem : list) {
                System.out.println("uid=" + uid + "is recommendered as " + recommendedItem);
            }
        }
    }

    public float estimateItemPreference(long userID, long itemID) throws Exception {
        Float actualPref = null;
        PreferenceArray preferencesFromUser = model.getPreferencesFromUser(userID);
        for (Preference p : preferencesFromUser) {
            if (p.getItemID() == itemID) {
                actualPref = p.getValue();
            }
        }
        if (actualPref != null) {
            return actualPref;
        }
        return doEstimateItemPreference(userID, preferencesFromUser, itemID);
    }

    protected float doEstimateItemPreference(long userID, PreferenceArray preferencesFromUser, long itemID)
            throws Exception {
        double preference = 0.0;
        double totalSimilarity = 0.0;
        int count = 0;
        double[] similarities = itemSimilarity.itemSimilarities(itemID, preferencesFromUser.getIDs());//得到与物品itemID相似物品的相似度
        for (int i = 0; i < similarities.length; i++) {
            double theSimilarity = similarities[i];
            if (!Double.isNaN(theSimilarity)) {
                // Weights can be negative!
                preference += theSimilarity * preferencesFromUser.getValue(i);//与物品itemID的第i个相似度*用户userID的第i个评分
                totalSimilarity += theSimilarity;
                count++;
            }
        }
        // Throw out the estimate if it was based on no data points, of course, but also if based on
        // just one. This is a bit of a band-aid on the 'stock' item-based algorithm for the moment.
        // The reason is that in this case the estimate is, simply, the user's rating for one item
        // that happened to have a defined similarity. The similarity score doesn't matter, and that
        // seems like a bad situation.
        if (count <= 1) {
            return Float.NaN;
        }
        float estimate = (float) (preference / totalSimilarity);
        if (capper != null) {
            estimate = capper.capEstimate(estimate);
        }
        return estimate;//userID对于itemID的评分
    }

    @Test
    public void testItemCF() throws Exception {//基于Item的协同过滤测试
        LongPrimitiveIterator iter = model.getUserIDs();
        while (iter.hasNext()) {
            long uid = iter.nextLong();
            Recommender recommender = new GenericItemBasedRecommender(model, itemSimilarity);
            List<RecommendedItem> list = recommender.recommend(uid, 3);
            for (RecommendedItem recommendedItem : list) {
                System.out.println("uid=" + uid + "is recommendered as " + recommendedItem);
            }
        }
    }
}
