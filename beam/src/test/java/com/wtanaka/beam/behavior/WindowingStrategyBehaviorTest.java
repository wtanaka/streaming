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
package com.wtanaka.beam.behavior;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.wtanaka.beam.functions.Identity;

import static com.wtanaka.beam.values.Timestamp.tv;

/**
 * Test that WindowingStrategy is updated in the new PCollection and
 * untouched in the old PCollection when you apply a <code>Bound</code>
 * PTransform
 */
public class WindowingStrategyBehaviorTest
{
   private static final int T1 = 1492400000;
   private static final int T2 = 1492400001;
   private static final int T3 = 1492400300;

   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   private PCollection<String> makeTimestampedInput()
   {
      List<TimestampedValue<String>> values = new ArrayList<>();
      values.add(tv("a", T1));
      values.add(tv("a", T2));
      values.add(tv("a", T3));
      return m_pipeline.apply(Create.timestamped(values));
   }

   /**
    * Test claims in https://wtanaka.com/node/8237
    */
   @Test
   public void testBoundUpdatesWindowingStrategy()
   {
      final PCollection<String> input = makeTimestampedInput();
      final WindowFn<?, ?> inputWindowFn =
         input.getWindowingStrategy().getWindowFn();
      Assert.assertTrue("Create.timestamped gave GlobalWindows",
         inputWindowFn.isCompatible(new GlobalWindows()));
      Assert.assertFalse("Sanity check, not compatible with SlidingWindows",
         inputWindowFn.isCompatible(
            SlidingWindows.of(Duration.standardMinutes(10))));

      final PCollection<String> windowed = input.apply(
         Window.into(FixedWindows.of(Duration.standardMinutes(4))));
      final WindowFn<?, ?> windowedFn =
         windowed.getWindowingStrategy().getWindowFn();
      Assert.assertFalse("Applying Window.Bound updates WindowingStrategy",
         windowedFn.isCompatible(new GlobalWindows()));
      Assert.assertTrue("Applying Window.Bound leaves input untouched",
         inputWindowFn.isCompatible(new GlobalWindows()));

      m_pipeline.run();
      Assert.assertTrue("Create.timestamped gave GlobalWindows",
         inputWindowFn.isCompatible(new GlobalWindows()));
   }

   /**
    * Test claims about Distinct in https://wtanaka.com/node/8237
    */
   @Test
   public void testDistinctDropsWindowingStrategy()
   {
      final PCollection<String> input = makeTimestampedInput();
      final PCollection<String> distinct = input
         .apply(Window.into(FixedWindows.of(Duration.standardMinutes(4))))
         .apply(Distinct.create());
      Assert.assertTrue(
         distinct.getWindowingStrategy().getWindowFn().isCompatible(
            FixedWindows.of(Duration.standardMinutes(4))));
      m_pipeline.run();
   }

   /**
    * https://wtanaka.com/node/8237 claims that InvalidWindows<W> -- some
    * grouping operations may return a PCollection with windowing strategy set
    * to InvalidWindows if the input windowing strategy is nonsensical for the
    * output
    */
   @Test
   public void testInvalidWindowsAfterGroupBy()
   {
      // Create a windowed input
      final PCollection<Integer> windowed = m_pipeline
         .apply(Create.timestamped(
            tv(1, 1),
            tv(1, 2),
            tv(1, 10),
            tv(1, 25)
         ))
         .apply(Window.into(FixedWindows.of(Duration.millis(10))));

      // Apply Combine.globally to it, and WindowFn is preserved
      final PCollection<Long> counts = windowed
         .apply(Combine.globally(Count.<Integer>combineFn())
            .withoutDefaults());
      Assert.assertFalse("Confirming that " + counts.getWindowingStrategy()
         .getWindowFn() + " is not InvalidWindows", counts
         .getWindowingStrategy()
         .getWindowFn() instanceof InvalidWindows);

      // Apply Combine.perKey to it
      final PCollection<KV<Integer, Iterable<Integer>>> groupByKey =
         windowed
            .apply(WithKeys.of(new Identity<>()))
            .setCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()))
            .apply(GroupByKey.create());
      Assert.assertFalse("Confirming that " + groupByKey
         .getWindowingStrategy()
         .getWindowFn() + " is not InvalidWindows", groupByKey
         .getWindowingStrategy().getWindowFn()
         instanceof InvalidWindows);
      m_pipeline.run();
   }
}
