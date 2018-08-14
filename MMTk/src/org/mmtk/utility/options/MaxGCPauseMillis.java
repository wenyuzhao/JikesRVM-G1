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

public class MaxGCPauseMillis extends org.vmutil.options.IntOption {
  /**
   * Create the option.
   */
  public MaxGCPauseMillis() {
    super(Options.set, "Max GC Pause Millis",
          "Sets a target value for desired maximum pause time. The specified value does not adapt to your heap size.",
          200);
  }

  /**
   * Ensure the value is valid.
   */
  @Override
  protected void validate() {
    failIf((this.value <= 0), "Ratio must be a positive integer");
  }
}
