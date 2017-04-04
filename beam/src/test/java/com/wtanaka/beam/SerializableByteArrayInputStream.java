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
import java.io.InputStream;
import java.io.Serializable;

/**
 * Serializable ByteArrayInputStream
 */
public class SerializableByteArrayInputStream extends InputStream implements
   Serializable
{
   private static final long serialVersionUID = 1L;
   private byte[] m_bytes;
   private int m_index = 0;

   public SerializableByteArrayInputStream(byte[] input)
   {
      m_bytes = input;
   }

   @Override
   public int read() throws IOException
   {
      if (m_index >= m_bytes.length)
      {
         return -1;
      }
      return m_bytes[m_index++];
   }
}
