package heap;

import chainexception.ChainException;

public class HeapFileException extends ChainException {
    public HeapFileException(Exception e, String s) {
        super(e,s);
    }
}
