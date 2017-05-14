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

import java.util.List;

import junit.framework.Assert;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class CombineOrderLimitTest
{
   private static final Top.Smallest REVERSE = new Top.Smallest();

   private static TestStream.Builder<KV<String, Integer>> newStreamBuilder()
   {
      return TestStream.create(KvCoder.of(StringUtf8Coder.of(),
         VarIntCoder.of()));
   }

   @Test
   public void testConstruct()
   {
      new CombineOrderLimit();
   }

   @Test
   public void testEmpty()
   {
      final TestPipeline p = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);
      final TestStream<KV<String, Integer>>
         stream = newStreamBuilder().advanceWatermarkToInfinity();

      final int limit = 1;

      final Window.Bound<KV<String, Integer>> triggering = Window
         .<KV<String, Integer>>triggering(
            Repeatedly.forever(AfterPane
               .elementCountAtLeast(1))).accumulatingFiredPanes();

      final PCollection<List<KV<String, Long>>> result = p
         // PCollection<KV<String, Integer>>
         .apply(stream)
         .apply(triggering)
         // PCollection<List<KV<String, Long>>> -- Smallest is actually
         // "reverse of natural order"
         .apply(CombineOrderLimit.of(Count.combineFn(), REVERSE, limit));

      PAssert.that(result).satisfies(
         (SerializableFunction<Iterable<List<KV<String, Long>>>, Void>)
            input ->
            {
               int count = 0;
               for (List<KV<String, Long>> x : input)
               {
                  count++;
                  Assert.assertEquals(0, x.size());
               }
               Assert.assertEquals(1, count);
               return null;
            });

      p.run();
   }
}
