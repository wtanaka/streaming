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

import java.io.PrintStream;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test MainRunner
 */
public class MainRunnerTest
{
   private static class PassThroughTransform extends
      PTransform<PCollection<byte[]>, PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         return input;
      }
   }

   @Test
   public void testCmdLine()
   {
      final byte[] bytes = {65, 10, 66, 10};
      final PrintStream oldOut = System.out;
      final SerializableByteArrayInputStream in =
         new SerializableByteArrayInputStream(bytes);
      try
      {
         SerializableByteArrayOutputStream out =
            new SerializableByteArrayOutputStream();
         assert 0 == SerializableByteArrayOutputStream.toByteArray().length;
         System.setOut(new PrintStream(out));
         MainRunner.cmdLine(StreamIO.readBound(in),
            StreamIO.write(out),
            new String[]{}, new PassThroughTransform());
         final byte[] result =
            SerializableByteArrayOutputStream.toByteArray();
         Assert.assertEquals(bytes.length, result.length);
      }
      finally
      {
         // Clean up the global variable
         SerializableByteArrayOutputStream.reset();
         System.setOut(oldOut);
      }
   }

   @Test
   public void testConstruct()
   {
      new MainRunner();
   }

   @Test
   public void testNullTransform()
   {
      try
      {
         MainRunner.cmdLine(null, null, null, null);
         Assert.fail("Expected exception raised");
      }
      catch (AssertionError e)
      {
      }
   }
}
