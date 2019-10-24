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
package org.mmtk.plan.g1.barrieranalysis.baseline;

import org.mmtk.utility.statistics.DoubleCounter;
import org.mmtk.utility.statistics.EventCounter;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class G1 extends org.mmtk.plan.g1.G1 {
  public static final boolean MEASURE_TAKERATE = true;
  public static final EventCounter barrierSlow;
  public static final EventCounter barrierFast;

  static {
    barrierSlow = new EventCounter("barrier.slow");
    barrierFast = new EventCounter("barrier.fast");
  }
}
