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
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * ByteArrayBoundedSource
 */
class ByteArrayBoundedSource extends BoundedSource<byte[]>
{
   private static final long serialVersionUID = 1L;
   private final byte[][] m_bytes;

   ByteArrayBoundedSource()
   {
      m_bytes = new byte[][]{};
   }

   /**
    * TODO: make immutable copy of bytes
    *
    * @param bytes source data
    */
   public ByteArrayBoundedSource(byte[][] bytes)
   {
      m_bytes = bytes;
   }

   @Override
   public BoundedReader<byte[]> createReader(
      final PipelineOptions options) throws IOException
   {
      return new ByteArrayBoundedReader(this, m_bytes);
   }

   @Override
   public Coder<byte[]> getDefaultOutputCoder()
   {
      return ByteArrayCoder.of();
   }

   @Override
   public long getEstimatedSizeBytes(final PipelineOptions options)
   {
      long count = 0;
      for (final byte[] m_byte : m_bytes)
      {
         count += m_byte.length;
      }
      return count;
   }

   /**
    * Currently only returns a single BoundedSource, but could be fixed to
    * use the start/skip constructor of ByteArrayBoundedReader.
    */
   @Override
   public List<? extends BoundedSource<byte[]>> splitIntoBundles(
      final long desiredBundleSizeBytes, final PipelineOptions options)
   {
      return Collections.singletonList(this);
   }

   @Override
   public void validate()
   {

   }

   /**
    * Source.Reader implementation for Stdin
    */
   static class ByteArrayBoundedReader extends BoundedReader<byte[]>
   {
      private final int m_amtToSkip;
      private final BoundedSource<byte[]> m_source;
      private final byte[][] m_bytes;
      private int m_curIndex = -1;

      ByteArrayBoundedReader(final BoundedSource<byte[]> source, byte[][]
         bytes)
      {
         this(source, bytes, 0, 1);
      }

      /**
       * Reader that reads some of the input.
       *
       * @param source the BoundedSource that created this
       * @param bytes source data
       * @param start the index to start at
       * @param skip the number of indices to skip on each read
       */
      ByteArrayBoundedReader(final BoundedSource<byte[]> source,
                             final byte[][] bytes, int start,
                             int skip)
      {
         m_amtToSkip = skip;
         m_curIndex = start - skip;
         m_source = source;
         m_bytes = bytes;
      }

      @Override
      public boolean advance()
      {
         return readNext();
      }

      @Override
      public void close()
      {

      }

      @Override
      public byte[] getCurrent() throws NoSuchElementException
      {
         if (m_curIndex >= m_bytes.length || m_curIndex < 0)
         {
            throw new NoSuchElementException(String.valueOf(m_curIndex));
         }
         return m_bytes[m_curIndex];
      }

      @Override
      public BoundedSource<byte[]> getCurrentSource()
      {
         return m_source;
      }

      private boolean readNext()
      {
         if (m_curIndex + m_amtToSkip >= m_bytes.length)
         {
            return false;
         }

         m_curIndex += m_amtToSkip;
         return true;
      }

      @Override
      public boolean start()
      {
         return readNext();
      }
   }

}
