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

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.functions.Compose;

/**
 * <p>Given a boolean filtering function, output the percentage of the input
 * PCollection that matches the filter.</p>
 * <p>This might be useful for calculating the error rate of a raw
 * web server log, by using a function like that checks for 4xx or 5xx
 * status codes.</p>
 */
public class PercentMatch
{
   private final static int TRUE = 1;
   private final static int FALSE = 0;
   private final static int ALL = 2;

   private static class Globally<T>
      extends PTransform<PCollection<T>, PCollection<Double>>
   {
      private final SerializableFunction<T, Boolean> m_function;

      private static class Divide
         extends DoFn<Iterable<KV<Integer, Long>>, Double>
      {
         @ProcessElement
         public void processElement(ProcessContext c)
         {
            long total = -1;
            long matching = 0;
            for (KV<Integer, Long> x : c.element())
            {
               switch (x.getKey())
               {
                  case TRUE:
                     assert matching == 0;
                     matching = x.getValue();
                     break;
                  case FALSE:
                     break;
                  case ALL:
                     assert total == -1;
                     total = x.getValue();
                     break;
               }
            }
            if (total > 0 && matching >= 0)
            {
               c.output(100.0 * matching / total);
            }
         }
      }

      Globally(final SerializableFunction<T, Boolean> function)
      {
         m_function = function;
      }

      @Override
      public PCollection<Double> expand(final PCollection<T> input)
      {
         return input
            .apply(ParDo.of(new Classify<>(m_function)))
            .apply(Keys.create())
            .apply(Count.perElement())
            .apply(WithKeys.of(0L))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(ParDo.of(new Divide()));
      }
   }

   private static class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Double>>>
   {
      private final SerializableFunction<V, Boolean> m_function;

      private static class Divide<K>
         extends DoFn<KV<K, Iterable<KV<Integer, Long>>>, KV<K, Double>>
      {
         @ProcessElement
         public void processElement(ProcessContext c)
         {
            long total = -1;
            long matching = 0;
            final KV<K, Iterable<KV<Integer, Long>>> element = c.element();
            for (KV<Integer, Long> x : element.getValue())
            {
               switch (x.getKey())
               {
                  case TRUE:
                     assert matching == 0;
                     matching = x.getValue();
                     break;
                  case FALSE:
                     break;
                  case ALL:
                     assert total == -1;
                     total = x.getValue();
                     break;
               }
            }
            if (total > 0 && matching >= 0)
            {
               c.output(KV.of(element.getKey(), 100.0 * matching / total));
            }
         }
      }

      PerKey(final SerializableFunction<V, Boolean> function)
      {
         m_function = function;
      }

      @Override
      public PCollection<KV<K, Double>> expand(
         final PCollection<KV<K, V>> input)
      {
         final Compose.Fn<KV<K, V>, Boolean> fnOfValueOf = Compose.of(
            m_function).apply(
            (SerializableFunction<KV<K, V>, V>) KV::getValue);

         return input
            // Add a key with the classification of the value of the KV
            .apply(ParDo.of(new Classify<>(fnOfValueOf)))
            // Make an aggregate key of K, plus the classification
            .apply("MapToKAndClassification", MapElements.<
               KV<Integer, KV<K, V>>, KV<K, Integer>>via(
               new SimpleFunction<KV<Integer, KV<K, V>>, KV<K, Integer>>()
               {
                  @Override
                  public KV<K, Integer> apply(
                     final KV<Integer, KV<K, V>> classified)
                  {
                     return KV.of(classified.getValue().getKey(),
                        classified.getKey());
                  }
               }))
            // Count KV<KV<K,Integer>, Long>
            .apply(Count.perElement())
            .apply(MapElements.via(
                     new SimpleFunction<KV<KV<K, Integer>, Long>,
                           KV<K, KV<Integer, Long>>>()
                     {
                        @Override
                        public KV<K, KV<Integer, Long>> apply(
                           final KV<KV<K, Integer>, Long> kvLongKV)
                        {
                           return KV.of(
                              kvLongKV.getKey().getKey(),
                              KV.of(kvLongKV.getKey().getValue(),
                                 kvLongKV.getValue()));
                        }
                     }))
            .apply(GroupByKey.create())
            .apply(ParDo.of(new Divide<>()));
      }
   }

   private static class Classify<T> extends DoFn<T, KV<Integer, T>>
   {
      private final SerializableFunction<T, Boolean> m_function;

      Classify(SerializableFunction<T, Boolean> function)
      {
         m_function = function;
      }

      @ProcessElement
      public void processElement(ProcessContext c)
      {
         T element = c.element();
         c.outputWithTimestamp(KV.of(m_function.apply(element)
            ? TRUE : FALSE, element), c.timestamp());
         c.outputWithTimestamp(KV.of(ALL, element), c.timestamp());
      }
   }

   public static <T> PTransform<
      PCollection<T>, PCollection<Double>> globally(
         SerializableFunction<T, Boolean> function)
   {
      return new Globally<>(function);
   }

   public static <K, V> PTransform<
      PCollection<KV<K, V>>, PCollection<KV<K, Double>>> perKey(
         SerializableFunction<V, Boolean> function)
   {
      return new PerKey<>(function);
   }
}
