package org.lenskit.mooc.uu;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleSortedMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;
import org.lenskit.api.Result;
import org.lenskit.api.ResultMap;
import org.lenskit.basic.AbstractItemScorer;
import org.lenskit.data.dao.DataAccessObject;
import org.lenskit.data.entities.CommonAttributes;
import org.lenskit.data.entities.CommonTypes;
import org.lenskit.data.ratings.Rating;
import org.lenskit.results.Results;
import org.lenskit.util.ScoredIdAccumulator;
import org.lenskit.util.TopNScoredIdAccumulator;
import org.lenskit.util.collections.LongUtils;
import org.lenskit.util.math.Scalars;
import org.lenskit.util.math.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

/**
 * User-user item scorer.
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleUserUserItemScorer extends AbstractItemScorer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleUserUserItemScorer.class);
    private final DataAccessObject dao;
    private final int neighborhoodSize;

    /**
     * Instantiate a new user-user item scorer.
     * @param dao The data access object.
     */
    @Inject
    public SimpleUserUserItemScorer(DataAccessObject dao) {
        this.dao = dao;
        neighborhoodSize = 30;
    }

    @Nonnull
    @Override
    public ResultMap scoreWithDetails(long user, @Nonnull Collection<Long> items) {
        // TODO Score the items for the user with user-user CF
        List<Result> results = new ArrayList<>();

        LongSet userLongSet = dao.getEntityIds(CommonTypes.USER);

        //get userMeanVector and euclideanNorm value
        Long2DoubleOpenHashMap userVector = getUserRatingVector(user);
        Long2DoubleOpenHashMap userMeanVector = meanCenterTheVector(userVector);

        //User Mean value
        double userMean = Vectors.mean(userVector);

        //Calculate prediction for user u to item i
        for (Long item : items) {
            PriorityQueue<UserCosine> neighbors = neighbor(userLongSet, item, user, userMeanVector);
            if(neighbors.size() < 2) {
                continue;
            }
            double score = computerScore(item, neighbors);
            score += userMean;
            results.add(Results.create(item, score));
        }

        //return final results
        return Results.newResultMap(results);
    }

    public double computerScore(Long item, PriorityQueue<UserCosine> neighbors) {
        double nominator = computeNominator(neighbors, item);
        double denominator = computeDenominator(neighbors);

        return nominator/denominator;
    }

    public double computeNominator(PriorityQueue<UserCosine> neighbors, Long item) {
        double result = 0;
        for (UserCosine uc : neighbors){
            Long2DoubleOpenHashMap itRatingVector = getUserRatingVector(uc.id);
            double rate = itRatingVector.get(item);
            result += uc.cosine*(rate-Vectors.mean(itRatingVector));
        }
        return result;
    }

    public double computeDenominator(PriorityQueue<UserCosine> neighbors) {
        double res = 0.0;
        for (UserCosine uc : neighbors) {
            res += uc.cosine;
        }
        return res;
    }

    public PriorityQueue<UserCosine> neighbor(LongSet userLongSet, Long item, Long user,
                                              Long2DoubleOpenHashMap userMeanVector) {
        PriorityQueue<UserCosine> minheap = new PriorityQueue<>(neighborhoodSize, new Comparator<UserCosine>() {
            @Override
            public int compare(UserCosine o1, UserCosine o2) {
                return o1.cosine > o2.cosine ? 1 : -1;
            }
        });

        for (Long other : userLongSet) {
            if (other != user) {
                //check if the user has rated the item before
                Long2DoubleOpenHashMap otherVector = getUserRatingVector(other);
                if (otherVector.containsKey(item)) {
                    //get the cosine similarity
                    Long2DoubleOpenHashMap otherMeanVector = meanCenterTheVector(otherVector);
                    double res = Vectors.dotProduct(userMeanVector, otherMeanVector);
                    res = res / (Vectors.euclideanNorm(userMeanVector)*Vectors.euclideanNorm(otherMeanVector));
                    if (res <= 0) {
                        continue;
                    }
                    if (minheap.size() < neighborhoodSize) {
                        minheap.offer(new UserCosine(other, res));
                    }
                    else if(minheap.peek().cosine < res){
                        minheap.poll();
                        minheap.offer(new UserCosine(other,res));
                    }
                }
            }
        }
        return minheap;
    }

    class UserCosine{
        Long id;
        Double cosine;
        public UserCosine(Long id, Double cosine) {
            this.id = id;
            this.cosine = cosine;
        }
    }

    private Long2DoubleOpenHashMap meanCenterTheVector(Long2DoubleOpenHashMap userRatingVector) {
        double mean = Vectors.mean(userRatingVector);

        // Get the Mutable Copy of the UserRatingVector for making the changes
        // in the values.
        Long2DoubleOpenHashMap mutUserRatingVector = new Long2DoubleOpenHashMap();
        for (Long2DoubleMap.Entry entry : userRatingVector.long2DoubleEntrySet()) {
            mutUserRatingVector.put(entry.getLongKey(), entry.getDoubleValue()-mean);
        }

        return mutUserRatingVector;
    }

    /**
     * Get a user's rating vector.
     * @param user The user ID.
     * @return The rating vector, mapping item IDs to the user's rating
     *         for that item.
     */
    private Long2DoubleOpenHashMap getUserRatingVector(long user) {
        List<Rating> history = dao.query(Rating.class)
                .withAttribute(CommonAttributes.USER_ID, user)
                .get();

        Long2DoubleOpenHashMap ratings = new Long2DoubleOpenHashMap();
        for (Rating r: history) {
            ratings.put(r.getItemId(), r.getValue());
        }

        return ratings;
    }

}
