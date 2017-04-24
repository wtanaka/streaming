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


import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * test StringToByteArray
 */
public class ByteArrayToStringTest
{
   @Test
   public void expand() throws Exception
   {
      final TestPipeline m_pipeline = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);
      final PCollection<String> strs =
         m_pipeline
            .apply(Create.of("a".getBytes(), "b".getBytes()))
            .apply(ByteArrayToString.of("UTF-8"));
      m_pipeline.run();
      PAssert.that(strs).containsInAnyOrder("a", "b");
   }

   @Test
   public void of()
   {
      ByteArrayToString.of("UTF-8");
   }
}
