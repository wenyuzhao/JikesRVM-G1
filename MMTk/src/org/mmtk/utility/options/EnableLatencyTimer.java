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

import org.mmtk.vm.VM;

/**
 * Should we enable a return barrier?
 */
public final class EnableLatencyTimer extends org.vmutil.options.BooleanOption {
  public EnableLatencyTimer() {
    super(Options.set, "Enable Latency Timer",
          "Should we enable a latency timer?",
        false);
  }

  @Override
  protected void validate() {
  }
}
