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

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test StringValueOf
 */
@RunWith(JUnit4.class)
public class StringValueOfTest
{
   @Rule
   public TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   @Test
   public void testExpand() throws Exception
   {
      final PCollection<Long> source = m_pipeline.apply(
         GenerateSequence.from(0L).to(10L));
      final PTransform<PCollection<Long>, PCollection<String>> transform =
         new StringValueOf<>();
      final PCollection<String> strings = source.apply(transform);
      PAssert.that(strings).containsInAnyOrder("1", "2", "3", "4", "5",
         "6", "7", "8", "9", "0");
      m_pipeline.run().waitUntilFinish();
   }

}
