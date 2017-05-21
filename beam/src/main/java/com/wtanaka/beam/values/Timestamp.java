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

/**
 * Helper function for values TimestampedValue
 */
public class Timestamp
{
   Timestamp()
   {

   }

   /**
    * Syntactical sugar to create a TimestampedValue for unit tests
    *
    * @param value
    * @param milliseconds
    * @param <V>
    * @return
    */
   public static <V> TimestampedValue<V> tv(V value, long milliseconds)
   {
      return tv(value, new Instant(milliseconds));
   }

   /**
    * Syntactical sugar to create a TimestampedValue for unit tests
    *
    * @param value
    * @param <V>
    * @param instant time
    * @return
    */
   public static <V> TimestampedValue<V> tv(V value, Instant instant)
   {
      return TimestampedValue.of(value, instant);
   }
}
