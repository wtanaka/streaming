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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * Chains two transforms together sequentially
 */
public class Sequential
{
   private static class Transform<InCollectionT extends PInput,
      IntermediateElementT, OutCollectionT extends POutput>
      extends PTransform<InCollectionT, OutCollectionT>
   {
      private static final long serialVersionUID = -1331633208390103689L;
      private final PTransform<InCollectionT,
         PCollection<IntermediateElementT>>
         m_first;
      private final PTransform<PCollection<IntermediateElementT>,
         OutCollectionT>
         m_second;

      Transform(
         PTransform<InCollectionT, PCollection<IntermediateElementT>> first,
         PTransform<PCollection<IntermediateElementT>, OutCollectionT> second)
      {
         m_first = first;
         m_second = second;
      }

      @Override
      public OutCollectionT expand(final InCollectionT input)
      {
         return Pipeline.applyTransform(input, m_first).apply(m_second);
      }
   }

   public static <InCollectionT extends PInput, IntermediateElementT,
      OutCollectionT extends POutput>
   PTransform<InCollectionT, OutCollectionT> of(
      PTransform<InCollectionT, PCollection<IntermediateElementT>> first,
      PTransform<PCollection<IntermediateElementT>, OutCollectionT> second)
   {
      return new Transform<>(first, second);
   }
}
