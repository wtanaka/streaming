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
package com.wtanaka.beam.functions;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Composed
 */
public class ComposedTest
{
   @Test
   public void testFunctionOfKvValue()
   {
      Assert.assertEquals("5", Composed
         .of((SerializableFunction<Integer, String>) String::valueOf)
         .apply((SerializableFunction<KV<String, Integer>, Integer>)
            KV::getValue)
         .apply(KV.of("hello", 5)));
   }

   @Test
   public void testSmoke()
   {
      final Composed<Double, String> doubleString = Composed
         .of((SerializableFunction<Integer, String>) String::valueOf)
         .apply((SerializableFunction<Double, Integer>)
            x1 -> (int) x1.doubleValue());
      Assert.assertEquals("1", doubleString.apply(1.2345));
   }
}
