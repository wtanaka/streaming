/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * StdinSource
 */
class StdinSource extends UnboundedSource<byte[],
   UnboundedSource.CheckpointMark>
{
   private static final long serialVersionUID = 1L;

   @Override
   public UnboundedReader<byte[]> createReader(
      final PipelineOptions options,
      @Nullable final CheckpointMark ignored)
      throws IOException
   {
      return new StdinReader(this);
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
      return "[StdinSource]";
   }

   @Override
   public void validate()
   {
   }
}
