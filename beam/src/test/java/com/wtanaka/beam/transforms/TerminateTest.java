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

import java.util.logging.Level;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PDone;
import org.junit.Test;

import com.wtanaka.beam.LoggingIO;

/**
 * Test {@link Terminate}
 */
public class TerminateTest
{
   @Test
   public void testConstruct()
   {
      new Terminate();
   }

   @Test
   public void testOf()
   {
      TestPipeline p
         = TestPipeline.create().enableAbandonedNodeEnforcement(true);
      final PDone result = p
         .apply(Create.of(1L, 2L, 3L))
         .apply(Terminate.of());
      p.run();
   }
}
