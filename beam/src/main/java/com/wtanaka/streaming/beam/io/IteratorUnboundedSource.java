package com.wtanaka.streaming.beam.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import com.wtanaka.streaming.beam.util.SerializableIterator;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Created by wtanaka on 12/20/16.
 */
public class IteratorUnboundedSource<OutputT extends Serializable> extends
   UnboundedSource
   implements Serializable
{
   private static final long serialVersionUID = -2597881632585819504L;
   private final SerializableIterator<OutputT> m_source;
   private final Class<OutputT> m_class;

   /**
    * @param source must be able to be accessed concurrently
    */
   public IteratorUnboundedSource(SerializableIterator<OutputT> source,
                                  Class<OutputT> clazz)
   {
      m_source = source;
      m_class = clazz;
   }

   @Override
   public List<? extends UnboundedSource> generateInitialSplits(
      final int desiredNumSplits, final PipelineOptions options)
      throws Exception
   {
      List<IteratorUnboundedSource> sources = newArrayList();

      for (int i = 0; i < desiredNumSplits; ++i)
      {
         sources.add(new IteratorUnboundedSource<OutputT>(m_source, m_class));
      }
      return sources;
   }

   @Override
   public UnboundedReader createReader(
      final PipelineOptions options,
      @Nullable final CheckpointMark checkpointMark)
      throws IOException
   {
      return new IteratorUnboundedSourceReader<OutputT>(m_source);
   }

   @Nullable
   @Override
   public Coder getCheckpointMarkCoder()
   {
      return null;
   }

   @Override
   public void validate()
   {

   }

   @Override
   public Coder<OutputT> getDefaultOutputCoder()
   {
      return SerializableCoder.of(m_class);
   }
}
