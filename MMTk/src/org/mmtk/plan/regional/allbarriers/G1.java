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
package org.mmtk.plan.regional.allbarriers;

import org.mmtk.utility.HeaderByte;
import org.mmtk.utility.deque.SharedDeque;
import org.vmmagic.pragma.Uninterruptible;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class G1 extends  org.mmtk.plan.regional.Regional {
    public final SharedDeque modbufPool = new SharedDeque("modBufs", metaDataSpace, 1);

    @Override
    public void collectionPhase(short phaseId) {
        if (phaseId == PREPARE) {
            G1Mutator.newMutatorBarrierActive = false;
            super.collectionPhase(PREPARE);
            modbufPool.clearDeque(1);
            HeaderByte.flip();
            return;
        }

        if (phaseId == EVACUATE_RELEASE) {
            super.collectionPhase(EVACUATE_RELEASE);
            G1Mutator.newMutatorBarrierActive = true;
            return;
        }

        super.collectionPhase(phaseId);
    }
}