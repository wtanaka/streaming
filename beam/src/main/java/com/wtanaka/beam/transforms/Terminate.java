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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Terminate a PTransform
 */
public class Terminate
{
   public static class Pipeline<T> extends PTransform<PCollection<T>, PDone>
   {
      @Override
      public PDone expand(final PCollection<T> input)
      {
         input.apply(ParDo.of(new DoFn<T, Void>()
         {
            @ProcessElement
            public void processElement(ProcessContext c)
            {
            }
         }));
         return PDone.in(input.getPipeline());
      }
   }

   public static <T> PTransform<PCollection<T>, PDone> of()
   {
      return new Pipeline<T>();
   }
}
