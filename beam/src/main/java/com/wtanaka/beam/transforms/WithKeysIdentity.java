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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.wtanaka.beam.functions.Identity;

/**
 * Call WithKeys using the identity function, to convert {@code T => KV<T, T>}
 */
public class WithKeysIdentity<T>
   extends PTransform<PCollection<T>, PCollection<KV<T, T>>>
{
   private static final long serialVersionUID = 1L;

   private WithKeysIdentity()
   {
   }

   public static <T> PTransform<PCollection<T>, PCollection<KV<T, T>>> of()
   {
      return new WithKeysIdentity<>();
   }

   @Override
   public PCollection<KV<T, T>> expand(final PCollection<T> input)
   {
      final Coder<T> oldCoder = input.getCoder();
      return input
         .apply(WithKeys.of(new Identity<>()))
         .setCoder(KvCoder.of(oldCoder, oldCoder));
   }
}
