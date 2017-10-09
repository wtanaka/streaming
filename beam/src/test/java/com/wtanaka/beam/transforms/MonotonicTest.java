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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.stream.Collectors;

import avro.shaded.com.google.common.collect.Ordering;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.wtanaka.beam.test.Recorder;

public class MonotonicTest
{
   private final static SerializableComparator<List<String>>
         COMPARE_SECOND_ITEM = new SerializableComparator<List<String>>()
   {
      @Override
      public int compare(List<String> o1, List<String> o2)
      {
         return o1.get(1).compareTo(o2.get(1));
      }
   };

   private final static SerializableFunction<KV<Integer, List<String>>, String>
         GET_FIRST_FROM_VALUE_LIST
         = new SerializableFunction<KV<Integer, List<String>>, String>()
   {
      @Override
      public String apply(KV<Integer, List<String>> input)
      {
         return input.getValue().get(0);
      }
   };

   private static class KVCallback implements
         SerializableFunction<KV<Integer, Integer>, KV<Integer, Integer>>
   {
      private final Recorder<KV<Integer, Integer>> m_recorder;

      public KVCallback(Recorder<KV<Integer, Integer>> recorder)
      {
         m_recorder = recorder;
      }

      @Override
      public KV<Integer, Integer> apply(KV<Integer, Integer> input)
      {
         m_recorder.add(input);
         return input;
      }
   }

   private static class IntCompare implements SerializableComparator<Integer>
   {
      @Override
      public int compare(Integer o1, Integer o2)
      {
         return o1.compareTo(o2);
      }
   }

   @Rule
   public transient TestPipeline m_pipeline = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);

   static KvCoder<Integer, Integer> kvIntIntCoder()
   {
      return KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
   }

   static TestStream.Builder<KV<Integer, Integer>> kvIntIntStream()
   {
      return TestStream.create(kvIntIntCoder());
   }

   static TestStream.Builder<KV<Integer, Integer>> populateElements(
         TestStream.Builder<KV<Integer, Integer>> stream, Random random,
         int uniqueKeyCount, int valuesPerKey, int maxValue)
   {
      for (int v = 0; v < valuesPerKey; ++v)
      {
         for (int k = 0; k < uniqueKeyCount; ++k)
         {
            stream = stream.addElements(KV.of(k, random.nextInt(maxValue)));
         }
      }
      return stream;
   }

   ArrayList<List<Integer>> collectResults(
         Recorder<KV<Integer, Integer>> recorder, int uniqueKeyCount)
   {
      Vector<KV<Integer, Integer>> result = recorder.get();
      ArrayList<List<Integer>> valueLists = new ArrayList<>();
      for (int i = 0; i < uniqueKeyCount; ++i)
      {
         final int current = i;
         List<Integer> filtered = result.stream()
               .filter(x -> current == x.getKey())
               .map(x -> x.getValue())
               .collect(Collectors.toList());
         valueLists.add(filtered);
      }
      return valueLists;
   }

   @Test
   public void testNonPrimitiveType()
   {
      PCollection<String> result = m_pipeline.apply(
            TestStream.create(KvCoder.of(VarIntCoder.of(),
                  ListCoder.of(StringUtf8Coder.of())))
                  .addElements(KV.of(1, Arrays.asList("a", "b", "c")),
                        KV.of(1, Arrays.asList("x", "y", "z")),
                        KV.of(1, Arrays.asList("c", "c", "c")),
                        KV.of(1, Arrays.asList("z", "z", "z")))
                  .advanceWatermarkToInfinity())
            .apply(Monotonic.weaklyIncreasing(
                  COMPARE_SECOND_ITEM, ListCoder.of(StringUtf8Coder.of()),
                  GET_FIRST_FROM_VALUE_LIST, StringUtf8Coder.of()));
      PAssert.that(result).containsInAnyOrder("a", "x", "z");
      m_pipeline.run();
   }

   @Test
   public void testStrictlyDecreasing()
   {
      final int uniqueKeyCount = 10;
      TestStream.Builder<KV<Integer, Integer>> stream = populateElements(
            kvIntIntStream(), new Random(0), uniqueKeyCount, 100,
            10);
      Recorder<KV<Integer, Integer>> recorder = new Recorder<>(
            "testStrictlyDecreasing");
      m_pipeline.apply(stream.advanceWatermarkToInfinity())
            .apply(Monotonic.strictlyDecreasing(new IntCompare(),
                  VarIntCoder.of(), new KVCallback(recorder),
                  kvIntIntCoder()));
      m_pipeline.run();
      for (List<Integer> filtered : collectResults(recorder, uniqueKeyCount))
      {
         Assert.assertTrue(Ordering.natural().reverse().isStrictlyOrdered
               (filtered));
      }
   }

   @Test
   public void testStrictlyIncreasing()
   {
      final int uniqueKeyCount = 10;
      TestStream.Builder<KV<Integer, Integer>> stream = populateElements(
            kvIntIntStream(), new Random(0), uniqueKeyCount, 100,
            10);
      Recorder<KV<Integer, Integer>> recorder = new Recorder<>(
            "testStrictlyIncreasing");
      m_pipeline.apply(stream.advanceWatermarkToInfinity())
            .apply(Monotonic.strictlyIncreasing(new IntCompare(),
                  VarIntCoder.of(), new KVCallback(recorder),
                  kvIntIntCoder()));
      m_pipeline.run();
      for (List<Integer> filtered : collectResults(recorder, uniqueKeyCount))
      {
         Assert.assertTrue(Ordering.natural().isStrictlyOrdered(filtered));
      }
   }

   /**
    * Test using comparators from {@link org.apache.beam.sdk.transforms.Top}
    */
   @Test
   public void testTopComparators()
   {
      Recorder<KV<Integer, Integer>> recorder = new Recorder<>(
            "testTopComparators");
      final PCollection<KV<Integer, Integer>> source = m_pipeline
            .apply(Create.empty(
                  KvCoder.of(VarIntCoder.of(), VarIntCoder.of())));
      // Mainly making sure this compiles
      source.apply("strictlyIncreasing",
            Monotonic.strictlyIncreasing(new Top.Largest(), VarIntCoder.of(),
                  new KVCallback(recorder), kvIntIntCoder()));
      source.apply("strictlyDecreasing",
            Monotonic.strictlyDecreasing(new Top.Largest(), VarIntCoder.of(),
                  new KVCallback(recorder), kvIntIntCoder()));
      source.apply("weaklyIncreasing",
            Monotonic.weaklyIncreasing(new Top.Largest(), VarIntCoder.of(),
                  new KVCallback(recorder), kvIntIntCoder()));
      source.apply("weaklyDecreasing",
            Monotonic.weaklyDecreasing(new Top.Largest(), VarIntCoder.of(),
                  new KVCallback(recorder), kvIntIntCoder()));
      // Needed because of @Rule check
      m_pipeline.run();
   }

   @Test
   public void testWeaklyDecreasing()
   {
      final int uniqueKeyCount = 10;
      TestStream.Builder<KV<Integer, Integer>> stream = populateElements(
            kvIntIntStream(), new Random(0), uniqueKeyCount, 100,
            10);
      Recorder<KV<Integer, Integer>> recorder = new Recorder<>(
            "testWeaklyDecreasing");
      m_pipeline.apply(stream.advanceWatermarkToInfinity())
            .apply(Monotonic.weaklyDecreasing(new IntCompare(),
                  VarIntCoder.of(), new KVCallback(recorder),
                  kvIntIntCoder()));
      m_pipeline.run();

      for (List<Integer> filtered : collectResults(recorder, uniqueKeyCount))
      {
         Assert.assertTrue(Ordering.natural().reverse().isOrdered(filtered));
         Assert.assertFalse(
               Ordering.natural().reverse().isStrictlyOrdered(filtered));
      }
   }

   @Test
   public void testWeaklyIncreasing()
   {
      final int uniqueKeyCount = 10;
      TestStream.Builder<KV<Integer, Integer>> stream = populateElements(
            kvIntIntStream(), new Random(0), uniqueKeyCount, 100,
            10);
      Recorder<KV<Integer, Integer>> recorder = new Recorder<>(
            "testWeaklyIncreasing");
      m_pipeline.apply(stream.advanceWatermarkToInfinity())
            .apply(Monotonic.weaklyIncreasing(new IntCompare(),
                  VarIntCoder.of(), new KVCallback(recorder),
                  kvIntIntCoder()));
      m_pipeline.run();
      for (List<Integer> filtered : collectResults(recorder, uniqueKeyCount))
      {
         Assert.assertTrue(Ordering.natural().isOrdered(filtered));
         Assert.assertFalse(
               Ordering.natural().isStrictlyOrdered(filtered));
      }
   }
}
