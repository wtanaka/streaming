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
package com.wtanaka.beam.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

/**
 * Unbounded source backed by a Java iterator, for use in playing with and
 * learning the Beam API.
 */
public class IteratorIO
{
   /**
    * UnboundedSource class that draws elements from an Iterator.  Possibly
    * useful for experimenting with Beam.
    */
   static class Unbound<T, IteratorT extends
      Iterator<TimestampedValue<T>> & Serializable>
      extends UnboundedSource<T, EmptyCheckpointMark>
      implements Serializable
   {
      private static final long serialVersionUID = -2597881632585819504L;
      private final IteratorT m_source;
      private final Coder<T> m_coder;

      /**
       * UnboundedSource.UnboundedReader implementation that uses an iterator
       * for elements
       */
      public static class Reader<T, IteratorT extends
         Iterator<TimestampedValue<T>> & Serializable> extends
         UnboundedReader<T>
      {
         private final IteratorT m_iterator;
         private final Unbound<T, IteratorT> m_source;
         private Instant m_watermark =
            BoundedWindow.TIMESTAMP_MIN_VALUE;
         private TimestampedValue<T> m_current = null;

         public Reader(IteratorT iterator,
                       final Unbound<T, IteratorT> source)
         {
            m_iterator = iterator;
            m_source = source;
         }

         @Override
         public boolean advance() throws IOException
         {
            if (m_iterator.hasNext())
            {
               try
               {
                  m_current = m_iterator.next();
                  m_watermark = Instant.now();
                  return true;
               }
               catch (NoSuchElementException e)
               {
               }
            }
            m_watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
            return false; // no more data is available
         }

         @Override
         public void close() throws IOException
         {
            m_current = null;
         }

         @Override
         public CheckpointMark getCheckpointMark()
         {
            return new EmptyCheckpointMark();
         }

         @Override
         public T getCurrent() throws NoSuchElementException
         {
            if (m_current == null)
            {
               throw new NoSuchElementException();
            }
            return m_current.getValue();
         }

         @Override
         public UnboundedSource<T, EmptyCheckpointMark> getCurrentSource()
         {
            return m_source;
         }

         @Override
         public Instant getCurrentTimestamp() throws NoSuchElementException
         {
            if (m_current == null)
            {
               throw new NoSuchElementException();
            }
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
         }

         @Override
         public Instant getWatermark()
         {
            return m_watermark;
         }

         @Override
         public boolean start() throws IOException
         {
            return this.advance();
         }
      }

      /**
       * @param source must be able to be accessed concurrently
       * @param coder
       */
      public Unbound(IteratorT source,
                     Coder<T> coder)
      {
         m_source = source;
         m_coder = coder;
      }

      @Override
      public UnboundedReader<T> createReader(
         final PipelineOptions options,
         @Nullable final EmptyCheckpointMark checkpointMark)
         throws IOException
      {
         return new Reader<>(m_source, this);
      }

      @Nullable
      @Override
      public Coder<EmptyCheckpointMark> getCheckpointMarkCoder()
      {
         return AvroCoder.of(EmptyCheckpointMark.class);
      }

      @Override
      public Coder<T> getDefaultOutputCoder()
      {
         return m_coder;
      }

      @Override
      public List<? extends UnboundedSource<T, EmptyCheckpointMark>> split(
         final int desiredNumSplits, final PipelineOptions options)
         throws Exception
      {
         List<UnboundedSource<T, EmptyCheckpointMark>> sources = new
            ArrayList<>();

         // Not actually parallelizable
//         for (int i = 0; i < desiredNumSplits; ++i)
//         {
         sources.add(new Unbound<>(m_source, m_coder));
//         }
         return sources;
      }

      @Override
      public void validate()
      {
      }
   }

   public static <T, IteratorT extends
      Iterator<TimestampedValue<T>> & Serializable>
   PTransform<PBegin, PCollection<T>> unboundRead(
      IteratorT iterator, Coder<T> defaultCoder)
   {
      return Read.from(new Unbound<>(iterator, defaultCoder));
   }
}
