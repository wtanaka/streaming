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
import java.io.OutputStream;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test MainRunner
 */
public class MainRunnerTest
{
   public static class PassthruTransform extends
      PTransform<PCollection<String>, PCollection<String>>
   {
      @Override
      public PCollection<String> expand(final PCollection<String> input)
      {
         return input;
      }
   }

   @Test
   public void testCmdLine()
   {
      final byte[] bytes = {65, 10, 66, 10};
      InputStream in = new SerializableByteArrayInputStream(bytes);
      try
      {
         SerializableByteArrayOutputStream out =
            new SerializableByteArrayOutputStream();
         assert 0 == out.toByteArray().length;
         MainRunner.cmdLine(Read.from(new StdinBoundedSource(in)),
            Write.to(new StdoutSink(out)),
            new String[]{}, new PassthruTransform());
         final byte[] result = out.toByteArray();
         Assert.assertEquals(bytes.length, result.length);
      }
      finally
      {
         SerializableByteArrayOutputStream.reset();
      }
   }
}
