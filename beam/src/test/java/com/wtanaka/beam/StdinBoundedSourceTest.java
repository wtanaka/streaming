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
import java.util.List;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for StdinBoundedSource
 */
public class StdinBoundedSourceTest
{
   private final PipelineOptions m_options = PipelineOptionsFactory.create();
   private StdinBoundedSource m_stdinSource;
   private StdinBoundedSource m_byteSource;

   private StdinBoundedSource.StdinBoundedReader createBytesReader()
   {
      return (StdinBoundedSource.StdinBoundedReader) m_byteSource
         .createReader(m_options);
   }

   private StdinBoundedSource.StdinBoundedReader createStdinReader()
   {
      return (StdinBoundedSource.StdinBoundedReader) m_stdinSource
         .createReader(m_options);
   }

   @Test
   public void getDefaultOutputCoder()
   {
      Assert.assertNotNull(m_stdinSource.getDefaultOutputCoder());
   }

   @Test
   public void getEstimatedSizeBytes()
   {
      m_stdinSource.getEstimatedSizeBytes(m_options);
   }

   @Before
   public void setUp()
   {
      m_stdinSource = new StdinBoundedSource();
      m_byteSource = new StdinBoundedSource(
         new SerializableByteArrayInputStream(new byte[]{65, 10}));
   }

   @Test
   public void splitIntoBundles()
   {
      final List<? extends BoundedSource<byte[]>>
         result = m_stdinSource.splitIntoBundles(100,
         m_options);

      Assert.assertEquals(1, result.size());
   }

   @Test
   public void testCreateReader()
   {
      createStdinReader();
   }

   @Test
   public void testReaderAdvance() throws IOException
   {
      createBytesReader().advance();
   }

   @Test
   public void validate() throws Exception
   {
      m_stdinSource.validate();
   }
}
