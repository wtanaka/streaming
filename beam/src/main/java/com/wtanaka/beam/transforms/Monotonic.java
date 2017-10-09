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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These transform let you consider only the elements in a PCollection which
 * form a monotonic sequence.
 */
public class Monotonic
{
   private final static Logger LOG = LoggerFactory.getLogger(Monotonic.class);
   private final static SerializableFunction<Integer, Boolean>
         PREDICATE_STRICTLY_DECREASING = i -> i > 0;
   private final static SerializableFunction<Integer, Boolean>
         PREDICATE_STRICTLY_INCREASING = i -> i < 0;
   private final static SerializableFunction<Integer, Boolean>
         PREDICATE_WEAKLY_DECREASING = i -> i >= 0;
   private final static SerializableFunction<Integer, Boolean>
         PREDICATE_WEAKLY_INCREASING = i -> i <= 0;

   private static class MyDoFn<K, V, OutT> extends DoFn<KV<K, V>, OutT>
   {
      private static final String STATE_ID = "mystate";
      private final SerializableFunction<KV<K, V>, OutT> m_callback;
      private final Comparator<V> m_comparator;
      private final SerializableFunction<Integer, Boolean> m_predicate;
      @StateId(STATE_ID)
      private final StateSpec<ValueState<V>> m_stateCell;

      private MyDoFn(Comparator<V> comparator,
            SerializableFunction<Integer, Boolean> comparatorPredicate,
            Coder<V> valueCoder,
            SerializableFunction<KV<K, V>, OutT> callback)
      {
         m_callback = callback;
         m_comparator = comparator;
         m_stateCell = StateSpecs.value(valueCoder);
         m_predicate = comparatorPredicate;
      }

      @ProcessElement
      public void process(ProcessContext c,
            @StateId(STATE_ID) ValueState<V> state)
      {
         final V savedValue = state.read();
         final V newValue = c.element().getValue();
         if (savedValue == null || m_predicate.apply(m_comparator.compare
               (savedValue, newValue)))
         {
            state.write(newValue);
            c.outputWithTimestamp(m_callback.apply(c.element()),
                  c.timestamp());
         }
      }
   }

   private static class Transform<K, V, OutT, ComparatorT extends
         Comparator<V> & Serializable>
         extends PTransform<PCollection<KV<K, V>>, PCollection<OutT>>
   {
      private final SerializableFunction<KV<K, V>, OutT> m_callback;
      private final ComparatorT m_comparator;
      private final Coder<OutT> m_outCoder;
      private final SerializableFunction<Integer, Boolean> m_predicate;
      private final Coder<V> m_valueCoder;

      private Transform(ComparatorT comparator,
            SerializableFunction<Integer, Boolean> comparatorPredicate,
            Coder<V> valueCoder, SerializableFunction<KV<K, V>, OutT> callback,
            Coder<OutT> outCoder)
      {
         m_valueCoder = valueCoder;
         m_outCoder = outCoder;
         m_comparator = comparator;
         m_predicate = comparatorPredicate;
         m_callback = callback;
      }

      @Override
      public PCollection<OutT> expand(PCollection<KV<K, V>> input)
      {
         return input.apply(ParDo.of(new MyDoFn<>(m_comparator, m_predicate,
               m_valueCoder,
               m_callback))).setCoder(m_outCoder);
      }
   }


   /**
    * Return a transform which traverses its input PCollection in strictly
    * decreasing order, only emitting elements which are smaller than a
    * previously seen element within a given key and window
    *
    * @param comparator the comparator that defines the ordering of V
    *       within each K
    * @param valueCoder the coder for the V type, e.g. used to create a
    *       State API cell
    * @param callback a callback function that gets called on a subset of
    *       elements which are in weakly increasing order
    * @param outCoder the coder for the OutT type
    * @param <K> key type
    * @param <V> these values are assumed to have an ordering
    * @param <OutT> the result value of the callback for the elements
    *       that get traversed
    * @param <ComparatorT> Serializable Comparator of V
    * @return a PTransform which calls the {@code callback} on a subset of
    *       elements in weakly increasing order
    */
   @Experimental(Experimental.Kind.STATE)
   public static <K, V, OutT, ComparatorT extends Comparator<V> & Serializable>
   PTransform<PCollection<KV<K, V>>,
         PCollection<OutT>> strictlyDecreasing(
         ComparatorT comparator, Coder<V> valueCoder,
         SerializableFunction<KV<K, V>, OutT> callback, Coder<OutT> outCoder)
   {
      return new Transform<>(comparator, PREDICATE_STRICTLY_DECREASING,
            valueCoder, callback, outCoder
      );
   }


   /**
    * Return a transform which traverses its input PCollection in strictly
    * increasing order, only emitting elements which are larger than a
    * previously seen element within a given key and window
    *
    * @param comparator the comparator that defines the ordering of V
    *       within each K
    * @param valueCoder the coder for the V type, e.g. used to create a
    *       State API cell
    * @param callback a callback function that gets called on a subset of
    *       elements which are in weakly increasing order
    * @param outCoder the coder for the OutT type
    * @param <K> key type
    * @param <V> these values are assumed to have an ordering
    * @param <OutT> the result value of the callback for the elements
    *       that get traversed
    * @param <ComparatorT> Serializable Comparator of V
    * @return a PTransform which calls the {@code callback} on a subset of
    *       elements in weakly increasing order
    */
   @Experimental(Experimental.Kind.STATE)
   public static <K, V, OutT, ComparatorT extends Comparator<V> & Serializable>
   PTransform<PCollection<KV<K, V>>, PCollection<OutT>> strictlyIncreasing(
         ComparatorT comparator, Coder<V> valueCoder,
         SerializableFunction<KV<K, V>, OutT> callback, Coder<OutT> outCoder)
   {
      return new Transform<>(comparator, PREDICATE_STRICTLY_INCREASING,
            valueCoder, callback, outCoder
      );
   }


   /**
    * Return a transform which traverses its input PCollection in weakly
    * decreasing order, dropping elements which are greater than a previously
    * seen element within a given key and window
    *
    * @param comparator the comparator that defines the ordering of V
    *       within each K
    * @param valueCoder the coder for the V type, e.g. used to create a
    *       State API cell
    * @param callback a callback function that gets called on a subset of
    *       elements which are in weakly increasing order
    * @param outCoder the coder for the OutT type
    * @param <K> key type
    * @param <V> these values are assumed to have an ordering
    * @param <OutT> the result value of the callback for the elements
    *       that get traversed
    * @param <ComparatorT> Serializable Comparator of V
    * @return a PTransform which calls the {@code callback} on a subset of
    *       elements in weakly increasing order
    */
   @Experimental(Experimental.Kind.STATE)
   public static <K, V, OutT, ComparatorT extends Comparator<V> & Serializable>
   PTransform<PCollection<KV<K, V>>,
         PCollection<OutT>> weaklyDecreasing(
         ComparatorT comparator, Coder<V> valueCoder,
         SerializableFunction<KV<K, V>, OutT> callback, Coder<OutT> outCoder)
   {
      return new Transform<>(comparator, PREDICATE_WEAKLY_DECREASING,
            valueCoder, callback, outCoder
      );
   }

   /**
    * Return a transform which traverses its input PCollection in weakly
    * increasing order, dropping elements which are less than a previously seen
    * element within a given key and window
    *
    * @param comparator the comparator that defines the ordering of V
    *       within each K
    * @param valueCoder the coder for the V type, e.g. used to create a
    *       State API cell
    * @param callback a callback function that gets called on a subset of
    *       elements which are in weakly increasing order
    * @param outCoder the coder for the OutT type
    * @param <K> key type
    * @param <V> these values are assumed to have an ordering
    * @param <OutT> the result value of the callback for the elements
    *       that get traversed
    * @param <ComparatorT> Serializable Comparator of V
    * @return a PTransform which calls the {@code callback} on a subset of
    *       elements in weakly increasing order
    */
   @Experimental(Experimental.Kind.STATE)
   public static <K, V, OutT, ComparatorT extends Comparator<V> & Serializable>
   PTransform<PCollection<KV<K, V>>,
         PCollection<OutT>> weaklyIncreasing(
         ComparatorT comparator, Coder<V> valueCoder,
         SerializableFunction<KV<K, V>, OutT> callback, Coder<OutT> outCoder)
   {
      return new Transform<>(comparator, PREDICATE_WEAKLY_INCREASING,
            valueCoder, callback, outCoder
      );
   }

}
