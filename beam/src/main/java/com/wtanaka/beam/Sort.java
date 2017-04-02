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

import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Implementation of sort
 */
public class Sort extends PTransform<PCollection<String>, PCollection<String>>
{
   private static final long serialVersionUID = 1L;

   @Override
   public PCollection<String> expand(final PCollection<String> input)
   {
      final SerializableFunction<String, String> fn =
         new SerializableFunction<String, String>()
         {
            private static final long serialVersionUID = 1L;

            @Override
            public String apply(final String input)
            {
               return input;
            }
         };
      PCollection<KV<String, String>> with2ndKey =
         input.apply("Pair with key", WithKeys.of(fn));
      final SerializableFunction<KV<String, String>, Integer> applyZero =
         new SerializableFunction<KV<String, String>, Integer>()
         {
            @Override
            public Integer apply(final KV<String, String> input)
            {
               return 0;
            }
         };
      PCollection<KV<Integer, KV<String, String>>> with1stKey =
         with2ndKey.apply("Add partition key", WithKeys.of(applyZero));
      final PCollection<KV<Integer, Iterable<KV<String, String>>>> grouped =
         with1stKey.apply("Group by partition key",
            GroupByKey.<Integer, KV<String, String>>create());
      // This is not distributed
      PCollection<KV<Integer, Iterable<KV<String, String>>>> sorted =
         grouped.apply("Sort", SortValues.<Integer, String, String>create(
            BufferedExternalSorter.options()));
      PCollection<Iterable<KV<String, String>>> secondaryKeys = sorted.apply(
         "Remove partition key", Values.create());
      PCollection<KV<String, String>> flattened = secondaryKeys.apply(
         "Flatten iterable", Flatten.iterables());
      PCollection<String> result = flattened.apply("Pull out values",
         Values.create());
      return result;
   }
}
