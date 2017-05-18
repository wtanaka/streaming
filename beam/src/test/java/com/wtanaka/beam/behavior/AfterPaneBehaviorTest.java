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

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Test AfterPane behavior per https://wtanaka.com/node/8241
 */
public class AfterPaneBehaviorTest
{
   @Test
   public void test()
   {
      final TestStream<Integer> stream = TestStream
         .create(VarIntCoder.of())
         .addElements(TimestampedValue.of(2, new Instant(1234L)))
         .addElements(TimestampedValue.of(3, new Instant(1235L)))
         .advanceWatermarkTo(new Instant(1234L))
         .advanceWatermarkTo(new Instant(1235L))
         .addElements(TimestampedValue.of(5, new Instant(1236L)))
         .addElements(TimestampedValue.of(7, new Instant(1237L)))
         .addElements(TimestampedValue.of(11, new Instant(1238L)))
         .advanceWatermarkTo(new Instant(1238L))
         .addElements(TimestampedValue.of(13, new Instant(1239L)))
         .advanceWatermarkTo(new Instant(1239L))
         .advanceWatermarkToInfinity();
      final Trigger trigger = Repeatedly.forever(
         AfterPane.elementCountAtLeast(1));
      final TestPipeline testPipeline = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);
      final PCollection<Integer> result =
         testPipeline
            .apply(stream)
            .apply(
               Window.<Integer>configure().triggering(trigger)
                  .accumulatingFiredPanes())
            .apply(Sum.integersGlobally());
//      result
//         .apply(new StringValueOf<>())
//         .apply(LoggingIO.write("AfterPaneBehavior", Level.WARNING));
      testPipeline.run();
      PAssert.that(result).containsInAnyOrder(2, 5, 10, 17, 28, 41, 41);
   }
}
