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
package org.mmtk.plan.regional.satbcond;

import org.mmtk.plan.Trace;
import org.vmmagic.pragma.Uninterruptible;

/**
 * This class implements the core functionality for a transitive
 * closure over the heap graph.
 */
@Uninterruptible
public class G1MarkTraceLocal extends org.mmtk.plan.regional.RegionalMarkTraceLocal {
  public G1MarkTraceLocal(Trace trace) {
    super(trace);
  }
}
