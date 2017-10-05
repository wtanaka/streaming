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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.functions.Identity;

/**
 * {@link FlattenKV} takes a {@code PCollection<K, Iterable<V>>} and
 * returns a {@code PCollection<KV<K, V>>} that contains all the elements
 * from each iterable.
 */
public class FlattenKV
{
   private static class Transform<K, V, OutputT> extends
         PTransform<PCollection<KV<K, Iterable<V>>>, PCollection<OutputT>>
   {
      private final SimpleFunction<KV<K, V>, OutputT> m_mapper;

      Transform(SimpleFunction<KV<K, V>, OutputT> mapper)
      {
         m_mapper = mapper;
      }

      @Override
      public PCollection<OutputT> expand(PCollection<KV<K, Iterable<V>>> input)
      {
         return input
               .apply(ParDo.of(new DoFn<KV<K, Iterable<V>>, KV<K, V>>()
               {
                  @ProcessElement
                  public void processElement(ProcessContext c)
                  {
                     K key = c.element().getKey();
                     for (V v : c.element().getValue())
                     {
                        c.outputWithTimestamp(KV.of(key, v), c.timestamp());
                     }
                  }
               }))
               .apply(MapElements.via(m_mapper));
      }
   }

   FlattenKV()
   {
   }

   public static <K, V> PTransform<PCollection<KV<K, Iterable<V>>>,
         PCollection<KV<K, V>>> iterables()
   {
      return new Transform<>(new Identity());
   }

   public static <K, V, OutputT> PTransform<PCollection<KV<K, Iterable<V>>>,
         PCollection<OutputT>> iterables(
         SimpleFunction<KV<K, V>, OutputT> mapper)
   {
      return new Transform<>(mapper);
   }
}
