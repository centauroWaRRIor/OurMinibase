package heap;

import java.util.Arrays;

public class Tuple extends Object
{
    public Tuple() {
    }

    public Tuple(byte[] byteArray, int startIndex, int length) {
        data = Arrays.copyOfRange(byteArray, startIndex, startIndex+length);
    }

    public int getLength() { 
        return data.length;
    }

    public byte[] getTupleByteArray() { 
        return data;
    }

    public byte [] data;
}

