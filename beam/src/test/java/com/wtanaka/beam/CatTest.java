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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test Cat class
 */
public class CatTest
{
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create();

   @Test
   public void testEmpty()
   {
      final PCollection<String> output =
         m_pipeline.apply(Create.empty(StringUtf8Coder.of()))
            .apply(new Cat.Transform());
      PAssert.that(output).empty();
      m_pipeline.run();
   }

   @Test
   public void testOne()
   {
      final Create.Values<String> values = Create.of("A");
      final PCollection<String> source = m_pipeline.apply(values);
      final PCollection<String> output = source.apply(new Cat.Transform());
      PAssert.that(output).containsInAnyOrder("A");
      m_pipeline.run();
   }

   @Test
   public void testMultiple()
   {
      final Create.Values<String> values = Create.of("A", "B", "C");
      final PCollection<String> source = m_pipeline.apply(values);
      final PCollection<String> output = source.apply(new Cat.Transform());
      PAssert.that(output).containsInAnyOrder("A", "B", "C");
      m_pipeline.run();
   }
}
