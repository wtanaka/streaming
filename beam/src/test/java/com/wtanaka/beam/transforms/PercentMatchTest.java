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
import java.util.logging.Level;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Test;

import com.wtanaka.beam.LoggingIO;
import com.wtanaka.beam.StringValueOf;

import static com.wtanaka.beam.values.Timestamp.tv;

/**
 * Test {@link PercentMatch}
 */
public class PercentMatchTest implements Serializable
{

   private final TestPipeline m_pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(true);

   @Test
   public void testByKey()
   {
      final PCollection<KV<String, Double>> result = m_pipeline
         .apply(Create
            .of(KV.of("a", 1), KV.of("a", 2), KV.of("a", 3), KV.of("a", 5),
               KV.of("a", 7), KV.of("b", 1), KV.of("b", 2)))
         .apply(PercentMatch.perKey(
            (SerializableFunction<Integer, Boolean>)
               integer -> integer % 2 == 1));

      PAssert.that(result).containsInAnyOrder(
         KV.of("a", 80.0), KV.of("b", 50.0));
      m_pipeline.run();
   }

   @Test
   public void testConstruct()
   {
      new PercentMatch();
   }

   @Test
   public void testPercentOdd()
   {
      final PCollection<Double> result = m_pipeline
         .apply(Create.of(1, 2, 3, 5, 7))
         .apply(PercentMatch.globally(
            (SerializableFunction<Integer, Boolean>)
               integer -> integer % 2 == 1));

      PAssert.thatSingleton(result).isEqualTo(80.0);
      m_pipeline.run();
   }

   @Test
   public void testWindowed()
   {
      final PCollection<Double> output = m_pipeline
         .apply(Create.timestamped(
            tv(500, 100L),
            tv(200, 101L),
            tv(200, 102L),
            tv(200, 110L),
            tv(200, 111L),
            tv(200, 112L),
            tv(500, 120L),
            tv(500, 121L),
            tv(500, 122L)
         ))
         .apply(Window.into(FixedWindows.of(Duration.millis(10L))))
         .apply(PercentMatch.globally(
            (SerializableFunction<Integer, Boolean>)
               integer -> integer == 500));

      PAssert.that(output).containsInAnyOrder
         (33.333333333333333, 0.0, 100.0);

      m_pipeline.run();
   }
}
