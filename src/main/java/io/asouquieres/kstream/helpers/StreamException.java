package io.asouquieres.kstream.helpers;

public class StreamException<T> {
    public Exception innerException;
    public T failingValue;

    public StreamException(Exception e, T v) {
        innerException = e;
        failingValue = v;
    }

    public static <T> StreamException<T> of(Exception e, T v) {
        return new StreamException<>(e,v);
    }

    @Override
    public String toString() {
        return "StreamException{" +
                "innerException=" + innerException.getMessage() +
                ", failingValue=" + failingValue +
                '}';
    }
}
