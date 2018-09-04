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
package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.concurrent.ConcurrentConstraints;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.vmmagic.pragma.Uninterruptible;

/**
 * SemiSpace common constants.
 */
@Uninterruptible
public class ShenandoahConstraints extends ConcurrentConstraints {
  @Override
  public boolean movesObjects() {
    return true;
  }
  @Override
  public boolean needsForwardAfterLiveness() {
    return true;
  }
  @Override
  public int gcHeaderBits() {
    return RegionSpace.LOCAL_GC_BITS_REQUIRED;
  }
  @Override
  public int gcHeaderWords() {
    return RegionSpace.GC_HEADER_WORDS_REQUIRED;
  }
  @Override
  public int numSpecializedScans() {
    return 2;
  }
  @Override
  public int maxNonLOSDefaultAllocBytes() {
    return Region.BYTES_IN_REGION;
  }



  public boolean needsObjectAddressComparisonBarrier() {
    return true;
  }

  public boolean needsJavaLangReferenceReadBarrier() {
    return true;
  }

  public boolean needsBooleanWriteBarrier() {
    return true;
  }

  public boolean needsBooleanReadBarrier() {
    return true;
  }

  public boolean booleanBulkCopySupported() {
    return true;
  }

  public boolean needsByteWriteBarrier() {
    return true;
  }

  public boolean needsByteReadBarrier() {
    return true;
  }

  public boolean byteBulkCopySupported() {
    return true;
  }

  public boolean needsCharWriteBarrier() {
    return true;
  }

  public boolean needsCharReadBarrier() {
    return true;
  }

  public boolean charBulkCopySupported() {
    return true;
  }

  public boolean needsShortWriteBarrier() {
    return true;
  }

  public boolean needsShortReadBarrier() {
    return true;
  }

  public boolean shortBulkCopySupported() {
    return true;
  }

  public boolean needsIntWriteBarrier() {
    return true;
  }

  public boolean needsIntReadBarrier() {
    return true;
  }

  public boolean intBulkCopySupported() {
    return true;
  }

  public boolean needsLongWriteBarrier() {
    return true;
  }

  public boolean needsLongReadBarrier() {
    return true;
  }

  public boolean longBulkCopySupported() {
    return true;
  }

  public boolean needsFloatWriteBarrier() {
    return true;
  }

  public boolean needsFloatReadBarrier() {
    return true;
  }

  public boolean floatBulkCopySupported() {
    return true;
  }

  public boolean needsDoubleWriteBarrier() {
    return true;
  }

  public boolean needsDoubleReadBarrier() {
    return true;
  }

  public boolean doubleBulkCopySupported() {
    return true;
  }

  public boolean needsWordWriteBarrier() {
    return true;
  }

  public boolean needsWordReadBarrier() {
    return true;
  }

  public boolean wordBulkCopySupported() {
    return true;
  }

  public boolean needsAddressWriteBarrier() {
    return true;
  }

  public boolean needsAddressReadBarrier() {
    return true;
  }

  public boolean addressBulkCopySupported() {
    return true;
  }

  public boolean needsExtentWriteBarrier() {
    return true;
  }

  public boolean needsExtentReadBarrier() {
    return true;
  }

  public boolean extentBulkCopySupported() {
    return true;
  }

  public boolean needsOffsetWriteBarrier() {
    return true;
  }

  public boolean needsOffsetReadBarrier() {
    return true;
  }

  public boolean offsetBulkCopySupported() {
    return true;
  }

  public boolean needsObjectReferenceWriteBarrier() {
    return true;
  }

  public boolean needsObjectReferenceReadBarrier() {
    return true;
  }

  public boolean needsObjectReferenceNonHeapWriteBarrier() {
    return true;
  }

  public boolean needsObjectReferenceNonHeapReadBarrier() {
    return true;
  }

  public boolean objectReferenceBulkCopySupported() {
    return true;
  }
}
