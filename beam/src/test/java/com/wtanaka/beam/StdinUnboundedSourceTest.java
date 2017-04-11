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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test StdinUnboundedSource
 */
public class StdinUnboundedSourceTest
{
   private StdinUnboundedSource m_source;
   private ByteArrayInputStream m_bytes;
   private StdinUnboundedSource.StdinUnboundedReader m_reader;

   @Test
   public void advance() throws Exception
   {
      Assert.assertTrue(m_reader.advance());
      Assert.assertTrue(m_reader.advance());
      Assert.assertFalse(m_reader.advance());
   }

   @Test
   public void close() throws IOException
   {
      m_reader.close();
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
   public void getCheckpointMark() throws Exception
   {
      Assert.assertNull(m_reader.getCheckpointMark());
   }

   @Test
   public void getCheckpointMarkCoder() throws Exception
   {
      final Coder<UnboundedSource.CheckpointMark>
         coder = m_source.getCheckpointMarkCoder();
      Assert.assertNull(coder);
   }

   @Test
   public void getCurrent() throws Exception
   {
      m_reader.start();
      Assert.assertArrayEquals(new byte[]{0x65, 0x0a}, m_reader.getCurrent
         ());
      m_reader.advance();
      Assert.assertArrayEquals(new byte[]{0x66, 0x0a}, m_reader.getCurrent
         ());
      Assert.assertFalse(m_reader.advance());
   }

   @Test
   public void getCurrentSource() throws Exception
   {
      Assert.assertNotNull(m_reader.getCurrentSource());
   }

   @Test
   public void getCurrentTimestamp() throws Exception
   {
      Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE,
         m_reader.getCurrentTimestamp());
   }

   @Test
   public void getDefaultOutputCoder() throws Exception
   {
      Assert.assertNotNull(m_source.getDefaultOutputCoder());
   }

   @Test
   public void getWatermark() throws Exception
   {
      Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE,
         m_reader.getWatermark());
   }

   @Before
   public void setUp() throws Exception
   {
      m_source = new StdinUnboundedSource();
      m_bytes = new ByteArrayInputStream(new byte[]{0x65, 0x0a, 0x66, 0x0a});
      m_reader = new StdinUnboundedSource.StdinUnboundedReader(new StdinUnboundedSource(), m_bytes);
   }

   @Test
   public void start() throws Exception
   {
      Assert.assertTrue(m_reader.start());
   }

   @Test
   public void testConstruct()
   {
      new StdinUnboundedSource.StdinUnboundedReader(null);
   }

   @Test
   public void validate() throws Exception
   {
      m_source.validate();
   }
}
