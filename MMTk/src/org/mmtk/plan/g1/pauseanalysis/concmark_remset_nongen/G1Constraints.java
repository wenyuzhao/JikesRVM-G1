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
package org.mmtk.plan.g1.pauseanalysis.concmark_remset_nongen;

import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class G1Constraints extends org.mmtk.plan.g1.pauseanalysis.G1Constraints {
  public boolean g1ConcurrentMarking() { return true; }
  public boolean g1RememberedSets() { return true; }
  public boolean g1ConcurrentRefinement() { return true; }
  public boolean g1HotCardOptimization() { return true; }
  public boolean g1GenerationalGC() { return false; }
  public boolean g1PauseTimePredictor() {
    return false;
  }
}
