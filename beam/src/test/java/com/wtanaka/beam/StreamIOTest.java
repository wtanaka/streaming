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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author $Author$
 * @version $Name$ $Date$
 **/
@RunWith(JUnit4.class)
public class StreamIOTest
{
   @Test
   public void testReadBound()
   {
      final Pipeline pipeline = TestPipeline.create();
      final ByteArrayInputStream bais = new ByteArrayInputStream(new byte[]
         {0x68, 0x0a, 0x65, 0x0a, 0x6c, 0x0a, 0x6c, 0x0a, 0x6f, 0x0a});
      PCollection<String> lines = pipeline.apply(
         new StreamIO.Read.Bound(bais));
      PAssert.that(lines).containsInAnyOrder("h", "e", "l", "l", "o");
   }

   @Test
   public void testWriteBound()
   {
      final Pipeline pipeline = TestPipeline.create();
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final PCollection<String> source = pipeline.apply(
         Create.of("foo", "bar", "baz"));
      source.apply(new StreamIO.Write.Bound(baos));
   }
}
