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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test Cat class
 */
public class CatTest
{
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void main() throws Exception
   {
      final InputStream oldIn = System.in;
      final PrintStream oldOut = System.out;
      try
      {
         final ByteArrayInputStream bais = new ByteArrayInputStream(
            new byte[]{0x01});
         System.setIn(bais);
         final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         final PrintStream newOut = new PrintStream(baos);
         System.setOut(newOut);

         Cat.main(new String[]{});

         Assert.assertArrayEquals(new byte[]{0x01}, baos.toByteArray());
      }
      finally
      {
         System.setIn(oldIn);
         System.setOut(oldOut);
      }
   }

   @Test
   public void testEmpty()
   {
      final PCollection<byte[]> output =
         m_pipeline.apply(Create.empty(ByteArrayCoder.of()))
            .apply(new Cat.Transform());
      PAssert.that(output).empty();
      m_pipeline.run();
   }

   @Test
   public void testMultiple()
   {
      final Create.Values<byte[]> values = Create.of(
         "A".getBytes(), "B".getBytes(), "C".getBytes());
      final PCollection<byte[]> source = m_pipeline.apply(values);
      final PCollection<byte[]> output = source.apply(new Cat.Transform());
      final PCollection<String> strOut = output.apply(
         new ByteArrayToString());
      PAssert.that(strOut).containsInAnyOrder("A", "B", "C");
      m_pipeline.run();
   }

   @Test
   public void testOne()
   {
      final Create.Values<byte[]> values = Create.of("A".getBytes());
      final PCollection<byte[]> source = m_pipeline.apply(values);
      final PCollection<byte[]> output = source.apply(new Cat.Transform());
      final PCollection<String> strOut = output.apply(
         new ByteArrayToString());
      PAssert.that(strOut).containsInAnyOrder("A");
      m_pipeline.run();
   }
}
