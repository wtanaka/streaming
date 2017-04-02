/*
 * Copyright (C) 2017 Wesley Tanaka <http://wtanaka.com>
 */
package com.wtanaka.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Implementation of nl
 */
public class Nl extends PTransform<PCollection<String>, PCollection<String>>
{
   private int m_lineNum = 0;

   @Override
   public PCollection<String> expand(final PCollection<String> input)
   {
      return input.apply(ParDo.of(new DoFn<String, String>()
      {
         @ProcessElement
         public void processElement(ProcessContext context)
         {
            String input = context.element();
            final String newString = String.valueOf(m_lineNum) + " " + input;
            context.output(newString);
         }
      }));
   }
}
