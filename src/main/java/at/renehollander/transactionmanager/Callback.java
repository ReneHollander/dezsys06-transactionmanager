package at.renehollander.transactionmanager;

public final class Callback {

    @FunctionalInterface
    public interface NoParamsWithError {
        void execute(Exception err);

        default void execute() {
            execute(null);
        }
    }

    @FunctionalInterface
    public interface NoParamsWithStringError {
        void execute(String[] err);

        default void execute() {
            execute(null);
        }
    }

    @FunctionalInterface
    public interface OneParamWithError<T> {

        void execute(Exception err, T t);

        default void execute(Exception e) {
            execute(e, null);
        }

        default void execute(T t) {
            execute(null, t);
        }
    }
}
