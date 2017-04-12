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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.lang3.ArrayUtils;

/**
 * <p>Serializable ByteArrayInputStream
 * <p>We use a static a.k.a. global variable here to get around the fact that
 * Beam will serialize instance fields in sinks.  Of course, this means that
 * this class is only usable in a single VM, e.g. using DirectRunner
 */
class SerializableByteArrayOutputStream extends OutputStream implements
   Serializable
{
   private static final long serialVersionUID = 1L;
   private static final ArrayList<Byte> s_bytes = new ArrayList<>();

   static byte[] toByteArray()
   {
      synchronized (SerializableByteArrayOutputStream.class)
      {
         final Byte[] array = s_bytes.toArray(new Byte[s_bytes.size()]);
         assert s_bytes.size() == array.length;
         final byte[] result = ArrayUtils.toPrimitive(array);
         assert s_bytes.size() == result.length;
         return result;
      }
   }

   static void reset()
   {
      synchronized (SerializableByteArrayOutputStream.class)
      {
         s_bytes.clear();
      }
   }

   @Override
   public void write(final int i) throws IOException
   {
      synchronized (SerializableByteArrayOutputStream.class)
      {
         s_bytes.add((byte) i);
      }
   }
}
