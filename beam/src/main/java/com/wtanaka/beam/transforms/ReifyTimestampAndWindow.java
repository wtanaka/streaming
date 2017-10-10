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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * {@link PTransform PTransforms} to reify the timestamp, window, and pane
 * information of elements.
 */
public class ReifyTimestampAndWindow
{
   /**
    * Private implementation of {@link Reify}
    */
   private static class ReifyDoFn<T> extends DoFn<T, ValueInSingleWindow<T>>
   {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window)
      {
         c.output(ValueInSingleWindow.of(
               c.element(), c.timestamp(), window, c.pane()));
      }
   }

   /**
    * Private implementation for {@link #of}.
    */
   private static class Reify<T> extends PTransform<
         PCollection<T>, PCollection<ValueInSingleWindow<T>>>
   {
      @Override
      public PCollection<ValueInSingleWindow<T>> expand(PCollection<T> input)
      {
         return input.apply(ParDo.of(new ReifyDoFn<>()))
               .setCoder(ValueInSingleWindow.Coder.of(
                     input.getCoder(),
                     input.getWindowingStrategy()
                           .getWindowFn()
                           .windowCoder()));
      }
   }

   ReifyTimestampAndWindow()
   {
   }

   /**
    * Create a {@link PTransform} that will reify the timestamp and {@link
    * org.apache.beam.sdk.transforms.windowing.PaneInfo} from {@link
    * org.apache.beam.sdk.transforms.DoFn.ProcessContext} and the {@link
    * BoundedWindow} from the {@link DoFn.ProcessElement} interface into a
    * {@link ValueInSingleWindow} wrapper object.
    *
    * @param <T> the type of element in the input {@link PCollection}
    */
   public static <T> PTransform<PCollection<T>,
         PCollection<ValueInSingleWindow<T>>> of()
   {
      return new Reify<>();
   }
}
