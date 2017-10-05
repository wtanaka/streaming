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

import avro.shaded.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FlattenKVTest
{
   private static class Plus extends SimpleFunction<KV<Long, Long>, Long>
   {
      @Override
      public Long apply(KV<Long, Long> input)
      {
         return input.getKey() + input.getValue();
      }
   }

   @Rule
   public transient TestPipeline m_pipeline = TestPipeline.create()
         .enableAbandonedNodeEnforcement(true);

   @Test
   public void testConstruct()
   {
      new FlattenKV();
   }

   @Test
   public void testIterablesEmptyIdentity()
   {
      PCollection<KV<Long, Long>> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of()))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables());
      PAssert.that(result).empty();
      m_pipeline.run();
   }

   @Test
   public void testIterablesEmptyMapper()
   {
      PCollection<Long> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of()))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables(new Plus()));
      PAssert.that(result).empty();
      m_pipeline.run();
   }

   @Test
   public void testIterablesOneItemIdentity()
   {
      PCollection<KV<Long, Long>> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of
                        (1234L)))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables());
      PAssert.that(result).containsInAnyOrder(KV.of(31337L, 1234L));
      m_pipeline.run();
   }

   @Test
   public void testIterablesOneItemMapper()
   {
      PCollection<Long> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of
                        (1234L)))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables(new Plus()));
      PAssert.that(result).containsInAnyOrder(31337L + 1234L);
      m_pipeline.run();
   }

   @Test
   public void testIterablesThreeItemsIdentity()
   {
      PCollection<KV<Long, Long>> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of
                        (1234L, 2345L, 3456L)))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables());
      PAssert.that(result).containsInAnyOrder(
            KV.of(31337L, 1234L),
            KV.of(31337L, 2345L),
            KV.of(31337L, 3456L)
      );
      m_pipeline.run();
   }

   @Test
   public void testIterablesThreeItemsMapper()
   {
      PCollection<Long> result = m_pipeline
            .apply(Create
                  .of(KV.<Long, Iterable<Long>>of(31337L, ImmutableList.of
                        (1234L, 2345L, 3456L)))
                  .withCoder(KvCoder.of(VarLongCoder.of(),
                        IterableCoder.of(VarLongCoder.of()))))
            .apply(FlattenKV.iterables(new Plus()));
      PAssert.that(result).containsInAnyOrder(
            31337L + 1234L,
            31337L + 2345L,
            31337L + 3456L
      );
      m_pipeline.run();
   }

}
