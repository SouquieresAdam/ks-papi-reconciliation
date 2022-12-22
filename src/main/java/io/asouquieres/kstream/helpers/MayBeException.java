package io.asouquieres.kstream.helpers;


/**
 * This class is a simple model that wrap either :
 * - a successful generic operation result
 * - the exception that occurred during the operation
 * @param <SV> operation Expected Result Type
 * @parem <FV> input failing value type
 */
public class MayBeException<SV, FV> {
    public StreamException<FV> streamException;
    public SV streamValue;

    private MayBeException(SV value) {
        this.streamValue = value;
    }

    private MayBeException(StreamException<FV> ex) {
        this.streamException = ex;
    }

    public static <SV,FV> MayBeException<SV,FV> of(SV v) {
        return new MayBeException<>(v);
    }

    public static <SV,FV> MayBeException<SV,FV> of(StreamException<FV> ex) {
        return new MayBeException<>(ex);
    }
}
