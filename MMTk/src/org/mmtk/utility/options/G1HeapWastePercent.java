/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.utility.options;

public class G1HeapWastePercent extends org.vmutil.options.FloatOption {
  /**
   * Create the option.
   */
  public G1HeapWastePercent() {
    super(Options.set, "G1 Heap Waste Percent",
          "Sets the percentage of heap that you are willing to waste. The Java VM does not initiate the mixed garbage collection cycle when the reclaimable percentage is less than the heap waste percentage.",
          10);
  }

  /**
   * Ensure the value is valid.
   */
  @Override
  protected void validate() {
    failIf((this.value < 0 || this.value > 100), "Ratio must be a float between 0 and 100");
  }
}
