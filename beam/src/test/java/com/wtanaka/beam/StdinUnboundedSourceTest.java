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

import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test StdinUnboundedSource
 */
public class StdinUnboundedSourceTest
{
   private StdinUnboundedSource m_source;

   @Before
   public void setUp() throws Exception
   {
      m_source = new StdinUnboundedSource();
   }

   @Test
   public void createReader() throws Exception
   {
      final UnboundedSource.UnboundedReader<byte[]>
         reader = m_source.createReader(null, null);
      Assert.assertNotNull(reader);
   }

   @Test
   public void generateInitialSplits() throws Exception
   {
      final List<? extends UnboundedSource<byte[], UnboundedSource
         .CheckpointMark>>
         splits = m_source.generateInitialSplits(1, null);
      Assert.assertTrue(splits.size() > 0);
   }

   @Test
   public void getCheckpointMarkCoder() throws Exception
   {
      final Coder<UnboundedSource.CheckpointMark>
         coder = m_source.getCheckpointMarkCoder();
      Assert.assertNull(coder);
   }

   @Test
   public void getDefaultOutputCoder() throws Exception
   {
      Assert.assertNotNull(m_source.getDefaultOutputCoder());
   }

   @Test
   public void validate() throws Exception
   {
      m_source.validate();
   }

}
