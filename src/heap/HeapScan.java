package heap;

import global.RID;
import heap.Tuple;

import chainexception.ChainException;

public class HeapScan extends Object {
    protected HeapScan(HeapFile hf) {
    }

    protected void finalize() throws Throwable {
    }

    public void close() throws ChainException {
    }

    public boolean hasNext() {
        return false;
    }

    public Tuple getNext(RID rid) {
        return null;
    }
}

