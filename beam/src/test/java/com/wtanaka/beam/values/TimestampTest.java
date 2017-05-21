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
package com.wtanaka.beam.values;

import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

import static com.wtanaka.beam.values.Timestamp.tv;

public class TimestampTest
{
   @Test
   public void testConstructor()
   {
      new Timestamp();
   }

   @Test
   public void testTv()
   {
      final TimestampedValue<String> result = tv("hi", 1234L);
      Assert.assertEquals("hi", result.getValue());
      Assert.assertEquals(new Instant(1234L), result.getTimestamp());
   }

   @Test
   public void testTvInstant()
   {
      tv("hi", Instant.now());
   }
}
