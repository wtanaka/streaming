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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Test Sequential
 */
public class SequentialTest
{
   @Test
   public void of()
   {
      final TestPipeline p =
         TestPipeline.create().enableAbandonedNodeEnforcement(true);
      final PTransform<PBegin, PCollection<Integer>> sequence =
         Sequential.of(Create.of(1, 2, 3), Sum.integersGlobally());
      final PCollection<Integer> sum = p.apply(sequence);
      p.run();
      PAssert.that(sum).containsInAnyOrder(6);
   }
}
