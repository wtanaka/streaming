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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test StdinUnboundedReader
 */
public class StdinUnboundedReaderTest
{
   private ByteArrayInputStream m_bytes;
   private StdinUnboundedReader m_reader;

   @Before
   public void setUp()
   {
      m_bytes = new ByteArrayInputStream(new byte[]{0x65, 0x0a, 0x66, 0x0a});
      m_reader = new StdinUnboundedReader(new StdinUnboundedSource(), m_bytes);
   }

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
   public void getCheckpointMark() throws Exception
   {
      Assert.assertNull(m_reader.getCheckpointMark());
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
   public void getWatermark() throws Exception
   {
      Assert.assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE,
         m_reader.getWatermark());
   }

   @Test
   public void start() throws Exception
   {
      Assert.assertTrue(m_reader.start());
   }

   @Test
   public void testConstruct()
   {
      new StdinUnboundedReader(null);
   }
}
