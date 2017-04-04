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

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * StdinUnboundedSource
 */
class StdinUnboundedSource extends UnboundedSource<byte[],
   UnboundedSource.CheckpointMark>
{
   private static final long serialVersionUID = 1L;

   @Override
   public UnboundedReader<byte[]> createReader(
      final PipelineOptions options,
      @Nullable final CheckpointMark ignored)
      throws IOException
   {
      return new StdinUnboundedReader(this);
   }

   @Override
   public List<? extends UnboundedSource<byte[], CheckpointMark>>
   generateInitialSplits(
      final int desiredNumSplits, final PipelineOptions options)
      throws Exception
   {
      return Collections.singletonList(this);
   }

   @Nullable
   @Override
   public Coder<CheckpointMark> getCheckpointMarkCoder()
   {
      return null;
   }

   @Override
   public Coder<byte[]> getDefaultOutputCoder()
   {
      return ByteArrayCoder.of();
   }

   @Override
   public String toString()
   {
      return "[StdinUnboundedSource]";
   }

   @Override
   public void validate()
   {
   }
}
