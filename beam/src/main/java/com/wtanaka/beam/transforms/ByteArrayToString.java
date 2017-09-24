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

import java.nio.charset.Charset;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Convert byte array to string
 */
public class ByteArrayToString
   extends PTransform<PCollection<byte[]>, PCollection<String>>
{
   private static final long serialVersionUID = 1L;
   private final MapFn m_mapFn;

   private static class MapFn extends SimpleFunction<byte[], String>
   {
      private final String m_charset;

      MapFn(String charset)
      {
         super();
         m_charset = charset;
      }

      @Override
      public String apply(final byte[] input)
      {
         return new String(input, Charset.forName(m_charset));
      }
   }

   private ByteArrayToString(String charset)
   {
      m_mapFn = new MapFn(charset);
   }

   public static ByteArrayToString of(String charset)
   {
      return new ByteArrayToString(charset);
   }

   @Override
   public PCollection<String> expand(final PCollection<byte[]> input)
   {
      return input.apply(MapElements.via(m_mapFn));
   }
}
