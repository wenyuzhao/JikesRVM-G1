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
package org.mmtk.plan.g1.barrieranalysis.satbuncond;

import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class G1Constraints extends org.mmtk.plan.g1.barrieranalysis.baseline.G1Constraints {
  @Override
  public boolean needsObjectReferenceWriteBarrier() {
    return true;
  }
  @Override
  public boolean needsJavaLangReferenceReadBarrier() {
    return true;
  }
  @Override
  public boolean g1ForceDrainModbuf() {
    return true;
  }
}
