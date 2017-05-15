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

import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.transforms.ByteArrayToString;
import com.wtanaka.beam.transforms.StringToByteArray;
import com.wtanaka.beam.transforms.WithKeysIdentity;

/**
 * Implementation of sort
 * <p>
 * yes | head -50 | nl | java -cp beam/build/libs/beam-all.jar
 * com.wtanaka.beam.Sort
 */
public class Sort
{
   public static class Transform extends PTransform<PCollection<byte[]>,
      PCollection<byte[]>>
   {
      private static final long serialVersionUID = 1L;

      @Override
      public PCollection<byte[]> expand(final PCollection<byte[]> input)
      {
         return input
            .apply(ByteArrayToString.of("UTF-8"))
            .apply("Pair with key", WithKeysIdentity.of())
            .apply("Add partition key", WithKeys.of(0))
            .apply("Group by partition key", GroupByKey.create())
            .setCoder(KvCoder.of(VarIntCoder.of(), IterableCoder
               .of(KvCoder
                  .of(StringUtf8Coder.of(), StringUtf8Coder.of()))))
            // This is not parallelizable
            .apply("Sort",
               SortValues.create(BufferedExternalSorter.options()))
            .apply("Remove partition key", Values.create())
            .apply("Flatten iterable", Flatten.iterables())
            .apply("Pull out values", Values.create())
            .apply(StringToByteArray.of("UTF-8"));
      }
   }

   public static void main(String[] args)
   {
      MainRunner.cmdLine(StreamIO.stdinBound(), StreamIO.stdout(),
         args, new Transform());
   }
}
