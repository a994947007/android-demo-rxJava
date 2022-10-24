package com.android.demo.rxandroid.observable;

import com.android.demo.rxandroid.observer.BaseObserver;
import com.android.demo.rxandroid.observer.Observer;

public class ObservableDistinctUntilChanged<T> extends AbstractObservableWithUpStream<T, T>{

    public ObservableDistinctUntilChanged(Observable<T> source) {
        super(source);
    }

    @Override
    public void subscribeActual(Observer<T> observer) {
        source.subscribe(new DistinctUntilChangedObserver<>(observer));
    }

    private static class DistinctUntilChangedObserver<T> extends BaseObserver<T, T> {

        private T last;

        public DistinctUntilChangedObserver(Observer<T> actual) {
            super(actual);
        }

        @Override
        public void onNext(T t) {
            if (t.equals(last)) {
                return;
            }
            actual.onNext(t);
        }
    }
}
