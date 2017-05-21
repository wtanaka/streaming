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
package com.wtanaka.beam;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import static com.wtanaka.beam.values.Timestamp.tv;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class StreamingWcTest
{
   @Rule
   public TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void mergeAccumulators() throws Exception
   {
      final StreamingWc.WcStats stats1 = new StreamingWc.WcStats(1, 2, 3);
      final StreamingWc.WcStats stats2 = new StreamingWc.WcStats(4, 5, 6);
      final List<StreamingWc.WcStats> stats = Arrays.asList(stats1,
         stats2);
      final StreamingWc.StatsCombineFn fn = new StreamingWc.StatsCombineFn();
      final StreamingWc.WcStats result = fn.mergeAccumulators(stats);
      Assert.assertEquals(5, result.getNumLines());
      Assert.assertEquals(7, result.getNumWords());
      Assert.assertEquals(9, result.getNumBytes());
   }

   @Test
   public void testConstruct()
   {
      new StreamingWc();
   }

   @Test
   public void testConstructor()
   {
      new StreamingWc();
   }

   @Test
   public void testEmpty()
   {
      m_pipeline
         .apply(Create.of(new byte[]{0x65}))
         .apply(new StreamingWc.Transform());
      m_pipeline.run();
   }

   @Test
   public void testMain()
   {
      final InputStream oldIn = System.in;
      final PrintStream oldOut = System.out;
      try
      {
         final byte[] bytes = {65, 10, 66, 10};
         InputStream in = new SerializableByteArrayInputStream(bytes);
         System.setIn(in);
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         System.setOut(new PrintStream(out));
         StreamingWc.main(new String[]{});
      }
      finally
      {
         System.setIn(oldIn);
         System.setOut(oldOut);
      }
   }

   @Test
   public void testPauseInMiddle()
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos));
      try
      {
         pw.println("hello world");
      }
      finally
      {
         pw.close();
      }
      final byte[] bytes = baos.toByteArray();

      final TestStream<byte[]> input = TestStream
         .create(ByteArrayCoder.of())
         .addElements(tv(bytes, 100))
         .advanceWatermarkTo(new Instant(0))
         .addElements(tv(bytes, 200))
         .advanceWatermarkTo(new Instant(1))
         .addElements(
            tv(bytes, 10000), tv(bytes, 10000), tv(bytes, 10000),
            tv(bytes, 10000), tv(bytes, 10000), tv(bytes, 10000),
            tv(bytes, 10000), tv(bytes, 10000))
         .advanceWatermarkToInfinity();

      final PCollection<byte[]> output =
         m_pipeline.apply(input).apply(new StreamingWc.Transform());

      output
         .apply(MapElements.into(strings()).via(
            (SerializableFunction<byte[], String>)
               b -> Arrays.toString(b)))
         .apply(LoggingIO.write("DEBUG", Level.SEVERE));

      PAssert.that(output).containsInAnyOrder(
         "1 2 12".getBytes(),
         "2 4 24".getBytes(),
         "10 20 120".getBytes(), // EARLY
         "10 20 120".getBytes() // ON TIME
      );
      m_pipeline.run();
   }
}
