package org.lenskit.mooc.ii;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.lenskit.api.Result;
import org.lenskit.api.ResultMap;
import org.lenskit.basic.AbstractItemScorer;
import org.lenskit.data.dao.DataAccessObject;
import org.lenskit.data.entities.CommonAttributes;
import org.lenskit.data.ratings.Rating;
import org.lenskit.results.Results;
import org.lenskit.util.ScoredIdAccumulator;
import org.lenskit.util.TopNScoredIdAccumulator;
import org.lenskit.util.collections.CollectionUtils;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.*;

/**
 * @author <a href="http://www.grouplens.org">GroupLens Research</a>
 */
public class SimpleItemItemScorer extends AbstractItemScorer {
    private final SimpleItemItemModel model;
    private final DataAccessObject dao;
    private final int neighborhoodSize;

    @Inject
    public SimpleItemItemScorer(SimpleItemItemModel m, DataAccessObject dao) {
        model = m;
        this.dao = dao;
        neighborhoodSize = 20;
    }

    /**
     * Score items for a user.
     * @param user The user ID.
     * @param items The score vector.  Its key domain is the items to score, and the scores
     *               (rating predictions) should be written back to this vector.
     */
    @Override
    public ResultMap scoreWithDetails(long user, @Nonnull Collection<Long> items) {
        Long2DoubleMap itemMeans = model.getItemMeans();
        Long2DoubleMap ratings = getUserRatingVector(user);

        // TODO Normalize the user's ratings by subtracting the item mean from each one.
        for (Map.Entry<Long, Double> entry : ratings.entrySet()) {
            Long key = entry.getKey();
            double mean = itemMeans.get(key);
            Double value = entry.getValue()-mean;
            ratings.put(key, value);
        }
        List<Result> results = new ArrayList<>();

        for (long item: items ) {
            // TODO Compute the user's score for each item, add it to results
            Long2DoubleMap neighbor = model.getNeighbors(item);

            double score = 0.0;
            double denominator = 0.0;
            double nominator = 0.0;
            int count = 0;
            PriorityQueue<Cell> pq = new PriorityQueue<>(neighborhoodSize, new Comparator<Cell>() {
                @Override
                public int compare(Cell o1, Cell o2) {
                    return o1.value > o2.value?1:-1;
                }
            });
            for (Map.Entry<Long, Double> entry : neighbor.entrySet()) {

                Long key = entry.getKey();
                Double cosine = entry.getValue();
                if (ratings.containsKey(key) && key != item) {
                    addTo(pq, key, cosine);
                }
            }

            for (Cell cell : pq) {
                denominator += cell.value;
                nominator += cell.value * ratings.get(cell.key);
            }


            score = nominator/denominator;
            score += itemMeans.get(item);
            results.add(Results.create(item, score));
        }

        return Results.newResultMap(results);

    }

    public void addTo(PriorityQueue<Cell> pq, Long key, Double value) {
        if (pq.size() < neighborhoodSize) {
            pq.offer(new Cell(key, value));
        }
        else if (pq.peek().value < value){
            pq.poll();
            pq.offer(new Cell(key, value));
        }
    }

    /**
     * Get a user's ratings.
     * @param user The user ID.
     * @return The ratings to retrieve.
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

    class Cell{
        Long key;
        Double value;
        public Cell(Long key, Double value) {
            this.key = key;
            this.value = value;
        }
    }
}
