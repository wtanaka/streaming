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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Convert string to byte
 */
public class StringToByteArray
   extends PTransform<PCollection<String>, PCollection<byte[]>>
{
   private static final long serialVersionUID = 1L;
   private final String m_charset;

   private StringToByteArray(String charset)
   {
      m_charset = charset;
   }

   public static StringToByteArray of(String charset)
   {
      return new StringToByteArray(charset);
   }

   @Override
   public PCollection<byte[]> expand(final PCollection<String> input)
   {
      return input.apply(MapElements.into(TypeDescriptor.of(byte[].class))
         .via((SerializableFunction<String, byte[]>) str -> str.getBytes(
            Charset.forName(m_charset))));
   }
}
