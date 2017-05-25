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
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Compose
 */
public class ComposeTest
{
   @Test
   public void testComposedComparator()
   {
      final Compose.Comp<String> comp = Compose.of(new Top.Largest<>(),
         (SerializableFunction<String, Long>) Long::parseLong);
      Assert.assertEquals(0, comp.compare("0", "0"));
      Assert.assertEquals(-1, comp.compare("0", "1"));
      Assert.assertEquals(1, comp.compare("1", "0"));
   }

   @Test
   public void testConstruct()
   {
      new Compose()
      {
      };
   }

   @Test
   public void testFunctionOfKvValue()
   {
      Assert.assertEquals("5", Compose
         .of((SerializableFunction<Integer, String>) String::valueOf)
         .apply((SerializableFunction<KV<String, Integer>, Integer>)
            KV::getValue)
         .apply(KV.of("hello", 5)));
   }

   @Test
   public void testSmoke()
   {
      final Compose.Fn<Double, String> doubleString = Compose
         .of((SerializableFunction<Integer, String>) String::valueOf)
         .apply((SerializableFunction<Double, Integer>)
            x1 -> (int) x1.doubleValue());
      Assert.assertEquals("1", doubleString.apply(1.2345));
   }
}
