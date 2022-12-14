package com.android.demo.rxandroid.observer;

public interface Subscriber<T> {
    void onNext(T t);

    void onError(Throwable r);

    void onComplete();
}
