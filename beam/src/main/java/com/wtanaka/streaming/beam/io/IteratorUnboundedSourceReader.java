package com.wtanaka.streaming.beam.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * UnboundedSource.UnboundedReader implementation that uses an iterator for
 * elements
 * Created by wtanaka on 12/20/16.
 */
public class IteratorUnboundedSourceReader<T> extends UnboundedSource
   .UnboundedReader<T>
{
   private final Iterator<T> m_iterator;
   private T m_current = null;

   public IteratorUnboundedSourceReader(Iterator<T> iterator)
   {
      m_iterator = iterator;
   }

   @Override
   public boolean advance() throws IOException
   {
      if (m_iterator.hasNext())
      {
         try
         {
            m_current = m_iterator.next();
            return true;
         }
         catch (NoSuchElementException e)
         {
            m_current = null;
         }
      }
      return false; // no more data is available
   }

   @Override
   public void close() throws IOException
   {
m_current = null;
   }

   @Override
   public UnboundedSource.CheckpointMark getCheckpointMark()
   {
      return null;
   }

   @Override
   public T getCurrent() throws NoSuchElementException
   {
      if (m_current == null)
      {
         throw new NoSuchElementException();
      }
      return m_current;
   }

   @Override
   public UnboundedSource<T, ?> getCurrentSource()
   {
      return null;
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
      return null;
   }

   @Override
   public boolean start() throws IOException
   {
      return this.advance();
   }
}
