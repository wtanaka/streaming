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

import org.apache.beam.sdk.values.KV;
import org.junit.Test;

/**
 * Test {@link KVComparator}
 */
public class KVComparatorTest
{
   @Test
   public void testConstructAnonymous()
   {
      new KVComparator<String, Double>()
      {
         private static final long serialVersionUID = -8612258289917881222L;

         @Override
         public int compare(final KV<String, Double> t1,
                            final KV<String, Double> t2)
         {
            return Double.compare(t1.getValue(), t2.getValue());
         }
      };
   }
}
