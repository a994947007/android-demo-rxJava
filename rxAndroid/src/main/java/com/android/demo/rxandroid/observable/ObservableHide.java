package com.android.demo.rxandroid.observable;

import com.android.demo.rxandroid.observer.BaseObserver;
import com.android.demo.rxandroid.observer.Observer;

public class ObservableHide<T> extends AbstractObservableWithUpStream<T, T>{
    public ObservableHide(Observable<T> source) {
        super(source);
    }

    @Override
    public void subscribeActual(Observer<T> observer) {
        source.subscribeActual(new HideObserver<>(observer));
    }

    private static class HideObserver<T> extends BaseObserver<T, T> {

        public HideObserver(Observer<T> actual) {
            super(actual);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
    }
}
