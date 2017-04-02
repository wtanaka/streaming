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

import java.io.Serializable;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateSpecs;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.MoreObjects;

/**
 * Test Nl Class
 * <p>
 * This test is Serializable, just so that it's easy to have anonymous
 * inner classes inside the non-static test methods.
 */
@RunWith(JUnit4.class)
public class NlTest implements Serializable
{
   @Rule
   public final transient TestPipeline m_pipeline = TestPipeline.create();

   @Test
   public void testEmpty()
   {
      final PCollection<String> output =
         m_pipeline.apply(Create.empty(StringUtf8Coder.of()))
            .apply(new Nl());
      PAssert.that(output).empty();
      m_pipeline.run();
   }

   @Test
   public void testOne()
   {
      final PCollection<String> source = m_pipeline.apply(Create.of("A"));
      final PCollection<String> output = source.apply(new Nl());
      PAssert.that(output).containsInAnyOrder("1\tA");
      m_pipeline.run();
   }

   @Test
   public void testThree()
   {
      final Create.Values<String> threeLetters = Create.of("A", "B", "C");
      final PCollection<String> source = m_pipeline.apply(threeLetters);
      final PCollection<String> output = source.apply(new Nl());
      // Not sure what order the lines came into Nl so we'll
      // split it apart again
      final PCollection<String> tokens = output.apply(Regex.split("\t"));
      PAssert.that(tokens).containsInAnyOrder("1", "A", "2", "B", "3", "C");
      m_pipeline.run();
   }
}
