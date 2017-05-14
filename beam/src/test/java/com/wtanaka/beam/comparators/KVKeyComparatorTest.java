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
package com.wtanaka.beam.comparators;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

public class KVKeyComparatorTest
{
   @Test
   public void testCompareByString()
   {
      final List<KV<String, Integer>> list = Arrays.asList(
         KV.of("c", 2), KV.of("b", 1), KV.of("a", 3));
      // Top.Largest actually means "natural order"
      list.sort(KVKeyComparator.of(new Top.Largest<String>()));
      Assert.assertEquals(Integer.valueOf(3), list.get(0).getValue());
      Assert.assertEquals(Integer.valueOf(1), list.get(1).getValue());
      Assert.assertEquals(Integer.valueOf(2), list.get(2).getValue());
   }
}
