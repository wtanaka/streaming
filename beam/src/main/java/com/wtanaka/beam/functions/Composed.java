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

/**
 * Function composition for SerializableFunction objects
 */
public abstract class Composed<InputT, OutputT>
   implements SerializableFunction<InputT, OutputT>
{
   public static <InputT, OutputT> Composed<InputT, OutputT> of
      (final SerializableFunction<InputT, OutputT> function)
   {
      return new Composed<InputT, OutputT>()
      {
         @Override
         public OutputT apply(final InputT inputT)
         {
            return function.apply(inputT);
         }
      };
   }

   public <PrelimT> Composed<PrelimT, OutputT> apply(
      final SerializableFunction<PrelimT, InputT> inner)
   {
      final Composed<InputT, OutputT> outer = this;
      return new Composed<PrelimT, OutputT>()
      {
         @Override
         public OutputT apply(final PrelimT input)
         {
            return outer.apply(inner.apply(input));
         }
      };
   }
}
