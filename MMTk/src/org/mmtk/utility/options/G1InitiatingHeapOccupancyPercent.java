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

public class G1InitiatingHeapOccupancyPercent extends org.vmutil.options.IntOption {
  /**
   * Create the option.
   */
  public G1InitiatingHeapOccupancyPercent() {
    super(Options.set, "G1 Initiating Heap Occupancy Percent",
          "Sets the Java heap occupancy threshold that triggers a marking cycle. The default occupancy is 45 percent of the entire Java heap.",
          45);
  }

  /**
   * Ensure the value is valid.
   */
  @Override
  protected void validate() {
    failIf((this.value < 0 || this.value > 100), "Ratio must be a float between 0 and 100");
  }
}
