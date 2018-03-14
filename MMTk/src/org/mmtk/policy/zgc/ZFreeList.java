package org.mmtk.policy.zgc;

import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;

import java.util.Iterator;

public class ZFreeList implements Iterable<Address> {
    private static final int NEXT_PAGE_POINTER_OFFSET = ZPage.NEXT_PAGE_POINTER_OFFSET;
    private static final int PREV_PAGE_POINTER_OFFSET = ZPage.PREV_PAGE_POINTER_OFFSET;

    private Address head = Address.zero(), tail = Address.zero();
    private int size = 0;

    public int size() {
        return size;
    }


    public static Address next(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && ZPage.isAligned(zPage));
        return zPage.plus(NEXT_PAGE_POINTER_OFFSET).loadAddress();
    }
    public static Address prev(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && ZPage.isAligned(zPage));
        return zPage.plus(PREV_PAGE_POINTER_OFFSET).loadAddress();
    }

    public static void link(Address prevPage, Address nextPage) {
        if (!prevPage.isZero()) {
            prevPage.plus(NEXT_PAGE_POINTER_OFFSET).store(nextPage);
        }
        if (!nextPage.isZero()) {
            nextPage.plus(PREV_PAGE_POINTER_OFFSET).store(prevPage);
        }
    }

    public static void unlink(Address prevPage, Address nextPage) {
        if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(ZPage.isAligned(prevPage) && ZPage.isAligned(nextPage));
            VM.assertions._assert(prevPage.isZero() || next(prevPage).EQ(nextPage));
            VM.assertions._assert(nextPage.isZero() || prev(nextPage).EQ(prevPage));
        }
        if (!nextPage.isZero()) nextPage.plus(PREV_PAGE_POINTER_OFFSET).store(Address.zero());
        if (!prevPage.isZero()) prevPage.plus(NEXT_PAGE_POINTER_OFFSET).store(Address.zero());
    }

    public void push(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && ZPage.isAligned(zPage));
        if (head.isZero()) {
            head = tail = zPage;
            return;
        } else if (tail.isZero()) {
            tail = zPage;
            link(head, tail);
            return;
        } else {
            link(tail, zPage);
            tail = zPage;
        }
        size += 1;
    }

    public void remove(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && ZPage.isAligned(zPage));
        Address prevPage = prev(zPage), nextPage = next(zPage);
        unlink(prevPage, zPage);
        unlink(zPage, nextPage);
        link(prevPage, nextPage);
        if (prevPage.isZero()) head = nextPage;
        if (nextPage.isZero()) tail = prevPage;
        size -= 1;
    }

    public Iterator<Address> iterator() {
        final ZFreeList list = this;
        return new Iterator<Address>() {
            Address next = list.head;

            @Override
            public boolean hasNext() {
                return !next.isZero();
            }

            @Override
            public Address next() {
                Address rtn = next;
                next = ZFreeList.next(next);
                return rtn;
            }
        };
    }
}
