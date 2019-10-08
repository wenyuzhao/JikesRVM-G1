package org.mmtk.utility;

import org.mmtk.vm.VM;
import org.vmmagic.pragma.Entrypoint;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class Atomic {
  @Uninterruptible
  public static class Int {
    @Entrypoint private volatile int value = 0;

    private static final Offset VALUE_OFFSET = VM.objectModel.getFieldOffset(Int.class, "value", int.class);

    @Inline
    public final void set(int v) {
      value = v;
//      ObjectReference.fromObject(this).toAddress().store(v);
    }

    @Inline
    public final int get() {
      return value;
//      return ObjectReference.fromObject(this).toAddress().prepareInt();
    }

    @Inline
    public final boolean attempt(int oldValue, int newValue) {
      Address pointer = ObjectReference.fromObject(this).toAddress().plus(VALUE_OFFSET);
      return pointer.attempt(oldValue, newValue);
    }

    @Inline
    public final int add(int inc) {
      Address pointer = ObjectReference.fromObject(this).toAddress().plus(VALUE_OFFSET);
      int oldValue, newValue;
      do {
        oldValue = pointer.prepareInt();
        newValue = oldValue + inc;
      } while (!pointer.attempt(oldValue, newValue));
      return oldValue;
    }

    @Inline
    public static int fetchAdd(Address pointer, int delta) {
      int oldValue, newValue;
      do {
        oldValue = pointer.prepareInt();
        newValue = oldValue + delta;
      } while (!pointer.attempt(oldValue, newValue));
      return oldValue;
    }

    @Inline
    public final void addNonAtomic(int value) {
      set(get() + value);
    }
  }

  @Uninterruptible
  public static class Long {
    @Entrypoint private volatile long value;

    private static final Offset VALUE_OFFSET = VM.objectModel.getFieldOffset(Long.class, "value", long.class);

    @Inline
    public final void set(long v) {
      value = v;
    }

    @Inline
    public final long get() {
      return value;
    }

    @Inline
    public final boolean attempt(long oldValue, long newValue) {
      return VM.memory.attemptLong(this, VALUE_OFFSET, oldValue, newValue);
    }

    @Inline
    public final long add(long value) {
      long oldValue, newValue;
      do {
        oldValue = VM.memory.prepareLong(this, VALUE_OFFSET);
        newValue = oldValue + value;
      } while (!attempt(oldValue, newValue));
      return oldValue;
    }
  }
}
