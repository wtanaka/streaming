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
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * <p>Implementation of a streaming version of StreamingWc that outputs
 * statistics periodically
 * <p>
 * yes hello world | java -cp beam/build/libs/beam-all.jar
 * com.wtanaka.beam.StreamingWc
 */
public class StreamingWc
{

   public static class Transform extends PTransform<PCollection<byte[]>,
      PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         final PCollection<byte[]> triggered = input.apply(
            Window.<byte[]>configure()
               .triggering(
                  Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
               .accumulatingFiredPanes());
         PCollection<Wc.Stats> stats = triggered.apply(
            Combine.globally(new Wc.StatsCombineFn()));
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
