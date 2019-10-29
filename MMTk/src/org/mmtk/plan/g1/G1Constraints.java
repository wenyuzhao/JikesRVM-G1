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
package org.mmtk.plan.g1;

import org.mmtk.plan.StopTheWorldConstraints;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RegionSpace;
import org.vmmagic.pragma.Uninterruptible;

/**
 * SemiSpace common constants.
 */
@Uninterruptible
public class G1Constraints extends StopTheWorldConstraints {
  @Override
  public boolean movesObjects() {
    return true;
  }
  @Override
  public boolean needsForwardAfterLiveness() {
    return false;
  }
  @Override
  public int gcHeaderBits() {
    return RegionSpace.LOCAL_GC_BITS_REQUIRED;
  }
  @Override
  public int gcHeaderWords() {
    return Validation.ENABLED ? 1 : 0;
  }
  @Override
  public int numSpecializedScans() {
    return 3;
  }
  @Override
  public int maxNonLOSDefaultAllocBytes() {
    return Region.MAX_ALLOC_SIZE;
  }

  // G1 Specific features
  public int g1LogPagesInRegion() { return 8; }
  public boolean g1ConcurrentMarking() { return true; }
  public boolean g1RememberedSets() { return true; }
  public boolean g1ConcurrentRefinement() { return true; }
  public boolean g1HotCardOptimization() { return true; }
  public boolean g1GenerationalGC() { return true; }
  public boolean g1PauseTimePredictor() {
    return true;
  }
  public boolean g1ForceDrainModbuf() { return false; }
  public boolean g1UseXorBarrier() { return true; }
  public float g1FixedNurseryRatio() { return 0.15f; }

  // Derived
  @Override public boolean needsConcurrentWorkers() { return g1ConcurrentMarking(); }
  @Override public boolean needsObjectReferenceWriteBarrier() { return g1ConcurrentMarking() || g1RememberedSets(); }
  @Override public boolean needsJavaLangReferenceReadBarrier() { return g1ConcurrentMarking(); }
}
