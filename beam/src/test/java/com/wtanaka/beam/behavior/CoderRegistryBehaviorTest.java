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
package com.wtanaka.beam.behavior;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Coder behavior per https://wtanaka.com/node/8232
 */
public class CoderRegistryBehaviorTest
{
   /**
    * Test https://wtanaka.com/node/8232
    *
    * @throws CannotProvideCoderException
    */
   @Test
   public void testGettingCoder() throws CannotProvideCoderException
   {
      final CoderRegistry registry = CoderRegistry.createDefault();
      Assert.assertEquals(ByteArrayCoder.of(), registry.getCoder
         (byte[].class));
   }
}
