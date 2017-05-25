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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Function composition for SerializableFunction objects
 */
public abstract class Compose
{
   public static abstract class Comp<T> implements Comparator<T>, Serializable
   {
   }

   public static abstract class Fn<InputT, OutputT>
      implements SerializableFunction<InputT, OutputT>
   {
      public <PrelimT> Fn<PrelimT, OutputT> apply(
         final SerializableFunction<PrelimT, InputT> inner)
      {
         final Fn<InputT, OutputT> outer = this;
         return new Fn<PrelimT, OutputT>()
         {
            @Override
            public OutputT apply(final PrelimT input)
            {
               return outer.apply(inner.apply(input));
            }
         };
      }
   }

   public static <T, CompT extends Comparator<T> & Serializable, X> Comp<X> of
      (final CompT comparator, SerializableFunction<X, T> fn)
   {
      return new Comp<X>()
      {
         @Override
         public int compare(final X a, final X b)
         {
            return comparator.compare(fn.apply(a), fn.apply(b));
         }
      };
   }

   public static <InputT, OutputT> Fn<InputT, OutputT> of
      (final SerializableFunction<InputT, OutputT> function)
   {
      return new Fn<InputT, OutputT>()
      {
         @Override
         public OutputT apply(final InputT inputT)
         {
            return function.apply(inputT);
         }
      };
   }
}
