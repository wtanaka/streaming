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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test ByteArrayBoundedSource
 */
public class ByteArrayBoundedSourceTest
{
   @Test
   public void createReader() throws IOException
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      source.createReader(PipelineOptionsFactory.create());
   }

   @Test
   public void getDefaultOutputCoder()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      Assert.assertNotNull(source.getDefaultOutputCoder());
   }

   @Test
   public void getEstimatedSizeBytes()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource(
         new byte[][]{new byte[]{1, 10}});
      Assert.assertEquals(2, source.getEstimatedSizeBytes
         (PipelineOptionsFactory.create()));
   }

   @Test
   public void splitIntoBundles()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource(
         new byte[][]{new byte[]{1, 10}});
      final List<? extends BoundedSource<byte[]>>
         bundleList = source.split(10,
         PipelineOptionsFactory.create());
      Assert.assertNotNull(bundleList);
   }

   @Test
   public void testReaderAdvance()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source, new
         byte[][]{new byte[]{65, 10}, new byte[]{66, 10}});
      reader.start();
      reader.advance();
      Assert.assertArrayEquals(new byte[]{66, 10}, reader.getCurrent());
   }

   @Test
   public void testReaderClose()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source,
         new byte[][]{});
      reader.close();
   }

   @Test
   public void testReaderGetCurrent()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source, new
         byte[][]{new byte[]{65, 10}});
      reader.start();
      reader.getCurrent();
   }

   @Test
   public void testReaderGetCurrentFails()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source,
         new byte[][]{});
      try
      {
         reader.getCurrent();
         Assert.fail();
      }
      catch (NoSuchElementException e)
      {
         // expected
      }
   }

   @Test
   public void testReaderGetSource()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source,
         new byte[][]{});
      Assert.assertSame(source, reader.getCurrentSource());
   }

   @Test
   public void testReaderStart()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource();
      final ByteArrayBoundedSource.ByteArrayBoundedReader reader = new
         ByteArrayBoundedSource.ByteArrayBoundedReader(source,
         new byte[][]{});
      reader.start();
   }

   @Test
   public void testSerializable() throws IOException
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource(
         new byte[][]{new byte[]{66, 10}});
      final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
      ObjectOutputStream oos = null;
      try
      {
         oos = new ObjectOutputStream(byteOutStream);
      }
      catch (IOException e)
      {
         Assert.fail();
      }
      oos.writeObject(source);
   }

   @Test
   public void validate()
   {
      final ByteArrayBoundedSource source = new ByteArrayBoundedSource(
         new byte[][]{new byte[]{1, 10}});
      source.validate();
   }
}
