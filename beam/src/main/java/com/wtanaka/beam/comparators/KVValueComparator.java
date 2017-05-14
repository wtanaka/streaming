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
package com.wtanaka.beam.comparators;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.beam.sdk.values.KV;

public class KVValueComparator
{
   private static class Impl<K, V, InnerCompareT extends Serializable &
      Comparator<V>>
      implements KVComparator<K, V>
   {
      private static final long serialVersionUID = 1L;
      private final InnerCompareT m_inner;

      private Impl(final InnerCompareT inner)
      {
         m_inner = inner;
      }

      @Override
      public int compare(final KV<K, V> kv1, final KV<K, V> kv2)
      {
         return m_inner.compare(kv1.getValue(), kv2.getValue());
      }
   }

   public static <K, V,
      InComparatorT extends Serializable & Comparator<V>>
   KVComparator<K, V> of(InComparatorT wrapped)
   {
      return new Impl(wrapped);
   }
}
