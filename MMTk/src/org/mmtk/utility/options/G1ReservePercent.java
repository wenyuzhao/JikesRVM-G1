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

public class G1ReservePercent extends org.vmutil.options.FloatOption {
  /**
   * Create the option.
   */
  public G1ReservePercent() {
    super(Options.set, "G1 Reserve Percent",
          "Sets the percentage of reserve memory to keep free so as to reduce the risk of to-space overflows. The default is 10 percent.",
          10f);
  }

  /**
   * Ensure the value is valid.
   */
  @Override
  protected void validate() {
    failIf((this.value < 0 || this.value > 100.0), "Ratio must be a float between 0 and 100");
  }
}
