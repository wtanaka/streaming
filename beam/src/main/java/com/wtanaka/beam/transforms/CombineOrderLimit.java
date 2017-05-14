/*
 * com.wtanaka.beam
 *
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com/>
 *
 * This program is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.wtanaka.beam.transforms;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.comparators.KVComparator;
import com.wtanaka.beam.comparators.KVValueComparator;

/**
 * <p>Groups the input collection by K, combine the values V within each
 * group using an aggregation, then finds either the largest or smallest
 * aggregated values.</p>
 * <p>
 * <p>Examples of this pattern:</p>
 * <ul>
 * <li>Given webserver logs {@code KV<IPAddress, LogEntry>},
 * look for highest-activity ip address with
 * CombineOrderLimit.of(Count.combineFn(), NATURAL_ORDER, 1)</li>
 * <li>Given individual scores (like NPS scores, customer service ratings,
 * votes) from a large number of users, find the top 5 average scores
 * with CombineOrderLimit.of(Mean.combineFn(), REVERSED_ORDER, 5)</li>
 * <li>Given sales data for products {@code KV<ProductId, DollarsPaid>},
 * find the bottom 5 selling products with CombineOrderLimit.of(Sum
 * .combineFn(), NATURAL_ORDER, 5)</li>
 * </ul>
 */
public class CombineOrderLimit
{
   /**
    * Implementation of {@link #of}
    */
   private static class Transform<K, V, AggregateT,
      ComparatorT extends Serializable & Comparator<AggregateT>>
      extends PTransform<PCollection<KV<K, V>>,
      PCollection<List<KV<K, AggregateT>>>>
   {
      private static final long serialVersionUID = 1L;
      private final CombineFn<V, ?, AggregateT> m_combineFn;
      private final KVComparator<K, AggregateT> m_comparator;
      private final int m_limit;

      Transform(CombineFn<V, ?, AggregateT> vCombineFn,
                ComparatorT comparator,
                int limit)
      {
         m_combineFn = vCombineFn;
         m_comparator = KVValueComparator.of(comparator);
         m_limit = limit;
      }

      @Override
      public PCollection<List<KV<K, AggregateT>>> expand(
         final PCollection<KV<K, V>> input)
      {
         return input
            // PCollection<KV<K, AggregateT>>
            .apply(Combine.perKey(m_combineFn))
            // PCollection<List<KV<K, AggregateT>>>
            .apply(Top.of(m_limit, m_comparator));
      }
   }

   /**
    * Return a transform that groups the input collection by key, then
    * combines them using the vCombineFn into some aggregate value, then
    * sorts the aggregates using the given comparator, then returns up to
    * the first {@param limit} keys.
    *
    * @param vCombineFn a function that aggregates all values for a key. For
    * example, this could be Count or Sum or Max or Mean.
    * @param comparator a function that defines a sort order for the results
    * of vCombineFn
    * @param <K> a partition key type
    * @param <V> the input value type
    * @param <AggregateT> an aggregate type that the vCombineFn returns.
    * Normally this would be something like Number or Double
    * @param <ComparatorT> usually this would be Top.Largest or Top.Smallest
    * @param limit how many results to return
    * @return PTransform
    */
   @Nonnull
   public static <K, V, AggregateT, ComparatorT extends Serializable &
      Comparator<AggregateT>>
   PTransform<PCollection<KV<K, V>>, PCollection<List<KV<K, AggregateT>>>> of(
      CombineFn<V, ?, AggregateT> vCombineFn,
      ComparatorT comparator,
      int limit)
   {
      return new Transform(vCombineFn, comparator, limit);
   }
}
