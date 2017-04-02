/*
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com>
 */
package com.wtanaka.beam;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Implementation of cat
 */
public class Cat extends PTransform<PCollection<String>, PCollection<String>>
{
   private static final long serialVersionUID = 4757131243436350384L;

   @Override
   public PCollection<String> expand(final PCollection<String> input)
   {
      return input;
   }
}
