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
package org.mmtk.plan.g1.concmark;

import org.mmtk.plan.Phase;
import org.mmtk.plan.Plan;
import org.mmtk.plan.TraceLocal;
import org.mmtk.utility.Log;
import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;

@Uninterruptible
public class G1Collector extends org.mmtk.plan.g1.baseline.G1Collector {}
