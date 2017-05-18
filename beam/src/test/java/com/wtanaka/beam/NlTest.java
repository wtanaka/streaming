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
import java.io.Serializable;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.wtanaka.beam.transforms.ByteArrayToString;

/**
 * Test Nl Class
 * <p>
 * This test is Serializable, just so that it's easy to have anonymous
 * inner classes inside the non-static test methods.
 */
@RunWith(JUnit4.class)
public class NlTest implements Serializable
{
   private static final long serialVersionUID = 1L;
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void testConstructor()
   {
      new Nl();
   }

   @Test
   public void testEmpty()
   {
      final PCollection<byte[]> output =
         m_pipeline.apply(Create.empty(ByteArrayCoder.of()))
            .apply(new Nl.Transform());
      PAssert.that(output).empty();
      m_pipeline.run();
   }

   @Test
   public void testMain()
   {
      final InputStream oldIn = System.in;
      final PrintStream oldOut = System.out;
      try
      {
         System.setIn(new ByteArrayInputStream(new byte[]{0x65, 0x0a,
            0x66, 0x0a}));
         final ByteArrayOutputStream baos = new ByteArrayOutputStream();
         System.setOut(new PrintStream(baos));
         Nl.main(new String[]{});
      }
      finally
      {
         System.setIn(oldIn);
         System.setOut(oldOut);
      }
   }

   @Test
   public void testOne()
   {
      final PCollection<byte[]> source = m_pipeline
         .apply(Create.of("A".getBytes()));
      final PCollection<byte[]> output = source.apply(new Nl.Transform());
      final PCollection<String> stringOut = output.apply(
         ByteArrayToString.of("UTF-8"));
      PAssert.that(stringOut).containsInAnyOrder("1\tA");
      m_pipeline.run();
   }

   @Test
   public void testThree()
   {
      final Create.Values<byte[]> threeLetters = Create.of(
         "A".getBytes(), "B".getBytes(), "C".getBytes());
      final PCollection<byte[]> source = m_pipeline.apply(threeLetters);
      final PCollection<byte[]> output = source.apply(new Nl.Transform());
      // Not sure what order the lines came into Nl so we'll
      // split it apart again
      final PCollection<String> tokens = output
         .apply(ByteArrayToString.of("UTF-8"))
         .apply(Regex.split("\t"));
      PAssert.that(tokens).containsInAnyOrder("1", "A", "2", "B", "3", "C");
      m_pipeline.run();
   }
}
