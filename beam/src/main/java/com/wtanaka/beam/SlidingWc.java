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

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

/**
 * <p>Implementation of sliding window word count
 * <p>
 * <p>
 * while :; do echo hello world; sleep 2; done | java -cp
 * beam/build/libs/beam-all.jar com.wtanaka.beam.SlidingWc
 */
public class SlidingWc
{
   public static class Transform extends PTransform<PCollection<byte[]>,
      PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         final PCollection<byte[]> windowed = input.apply(
            Window.into(SlidingWindows.of(Duration.standardSeconds
               (10)).every(Duration.standardSeconds(1))));
         PCollection<Wc.Stats> stats = windowed.apply(
            Combine.globally(new Wc.StatsCombineFn()).withoutDefaults());
         return stats.apply(MapElements
            .into(TypeDescriptor.of(byte[].class))
            .via(Wc::bytesFor));
      }
   }

   public static void main(String[] args)
   {
      MainRunner.cmdLine(args, new Transform());
   }
}
