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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Source.Reader implementation for Stdin
 */
class StdinUnboundedReader extends UnboundedSource.UnboundedReader<byte[]>
{
   final private UnboundedSource<byte[], ?> m_source;
   private final InputStream m_stream;
   private ByteArrayOutputStream m_buffer = new ByteArrayOutputStream();

   StdinUnboundedReader(final UnboundedSource<byte[], ?> source)
   {
      this(source, null);
   }

   StdinUnboundedReader(final UnboundedSource<byte[], ?> source, InputStream stream)
   {
      m_source = source;
      m_stream = stream;
   }

   private InputStream getStream()
   {
      return m_stream == null ? System.in : m_stream;
   }

   @Override
   public boolean advance() throws IOException
   {
      return readNext();
   }

   @Override
   public void close() throws IOException
   {
   }

   @Override
   public UnboundedSource.CheckpointMark getCheckpointMark()
   {
      return null;
   }

   @Override
   public byte[] getCurrent() throws NoSuchElementException
   {
      return m_buffer.toByteArray();
   }

   @Override
   public UnboundedSource<byte[], ?> getCurrentSource()
   {
      return m_source;
   }

   @Override
   public Instant getCurrentTimestamp() throws NoSuchElementException
   {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
   }

   @Override
   public Instant getWatermark()
   {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
   }

   private boolean readNext() throws IOException
   {
      m_buffer.reset();
      int ch = getStream().read();
      if (ch == -1)
      {
         return false;
      }
      assert (int)'\n' == 10;
      while (ch != -1 && ch != (int)'\n')
      {
         m_buffer.write(ch);
         ch = getStream().read();
      }
      if (ch != -1)
      {
         m_buffer.write(ch);
      }
      return true;
   }

   @Override
   public boolean start() throws IOException
   {
      return readNext();
   }
}
