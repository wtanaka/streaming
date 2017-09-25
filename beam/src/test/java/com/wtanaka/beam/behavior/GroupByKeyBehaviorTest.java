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
package com.wtanaka.beam.behavior;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.LongStream;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Lists;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;

/**
 * Checks for various behavior of GroupByKey
 */
public class GroupByKeyBehaviorTest implements Serializable
{
   @Rule
   public transient TestPipeline m_pipeline = TestPipeline.create()
      .enableAbandonedNodeEnforcement(true);

   /**
    * A user@ mailing list post referred to using side inputs to convert a
    * small PCollection into a list.
    * {@code https://lists.apache.org/thread.html
    * /8fa1b84f38ec9b42d30b6ed82c24cc3b18edd7fb9a309d91eec65050@
    * <user.beam.apache.org>}
    * <p>This code tests another approach of assigning each element the same
    * key, then using {@link GroupByKey}
    */
   @Test
   public void testPCollectionToIterator()
   {
      PCollection<ArrayList<Long>> result
         = m_pipeline.apply(GenerateSequence.from(0L).to(10L))
         .apply(WithKeys.of(31337))
         .apply(GroupByKey.create())
         .apply(Values.create())
         .apply(MapElements
            .into(new TypeDescriptor<ArrayList<Long>>() {})
            .via(iterable -> Lists.newArrayList(iterable)))
         .apply("sort", MapElements
            .into(new TypeDescriptor<ArrayList<Long>>() {})
            .via((SerializableFunction<ArrayList<Long>, ArrayList<Long>>)
               list -> {
                  ArrayList<Long> sortedList = Lists.newArrayList(list);
                  Collections.sort(sortedList);
                  return sortedList;
               }));

      PAssert.that(result)
         .containsInAnyOrder(
            Lists.newArrayList(LongStream.range(0L, 10L).iterator()));
      m_pipeline.run();
   }
}
