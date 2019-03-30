package org.mmtk.plan.regional.remsetbarrier.fastxorfastg1;

import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.regional.remsetbarrier.G1Mutator {
    @Inline
    @Override
    public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
//        if (!ref.isNull() && Space.isInSpace(G1.RS, ref)) {
            Word x = VM.objectModel.refToAddress(src).toWord();
            Word y = VM.objectModel.refToAddress(ref).toWord();
            Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
            tmp = ref.isNull() ? Word.zero() : tmp;
            if (!tmp.isZero() && Space.isInSpace(G1.RS, ref)) {
                markAndEnqueueCard(src);
            }
//        }
    }
}
