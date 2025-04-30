package com.android.demo.rxandroid.observable;

import com.android.demo.rxandroid.observer.Action;
import com.android.demo.rxandroid.observer.BaseObserver;
import com.android.demo.rxandroid.observer.Observer;

public class ObservableDispose<T> extends AbstractObservableWithUpStream<T, T>{

    private final Action onDispose;

    public ObservableDispose(Observable<T> source, Action onDispose) {
        super(source);
        this.onDispose = onDispose;
    }

    @Override
    public void subscribeActual(Observer<T> observer) {
        source.subscribeActual(new DoOnDisposeObserver<>(observer, onDispose));
    }

    private static final class DoOnDisposeObserver<T> extends BaseObserver<T, T> {

        private final Action onDispose;

        public DoOnDisposeObserver(Observer<T> actual, Action onDispose) {
            super(actual);
            this.onDispose = onDispose;
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void dispose() {
            super.dispose();
            onDispose.run();
        }
    }
}
