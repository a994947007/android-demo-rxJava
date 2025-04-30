package com.android.demo.rxandroid.observable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.android.demo.rxandroid.disposable.Disposable;
import com.android.demo.rxandroid.filter.Predicate;
import com.android.demo.rxandroid.function.Function;
import com.android.demo.rxandroid.observer.Action;
import com.android.demo.rxandroid.observer.BlockingFirstObserver;
import com.android.demo.rxandroid.observer.BlockingLastObserver;
import com.android.demo.rxandroid.observer.BlockingObserver;
import com.android.demo.rxandroid.observer.Consumer;
import com.android.demo.rxandroid.observer.LambdaObserver;
import com.android.demo.rxandroid.observer.Observer;
import com.android.demo.rxandroid.observer.Subscriber;
import com.android.demo.rxandroid.plugin.Functions;
import com.android.demo.rxandroid.plugin.RxJavaPlugins;
import com.android.demo.rxandroid.schedule.Scheduler;
import com.android.demo.rxandroid.schedule.Schedules;

public abstract class Observable<T> implements ObservableSource<T>{
    public interface OnSubscriber<T> {
        void call(Subscriber<T> subscriber);
    }

    @Override
    public void subscribe(Observer<T> observer) {
        subscribeActual(observer);
    }

    public abstract void subscribeActual(Observer<T> observer);

    public static <T> Observable<T> create(OnSubscriber<T> onSubscriber) {
        return new ObservableCreate<>(onSubscriber);
    }

    @SafeVarargs
    public static <T> Observable<T> just(T... t) {
        return new ObservableJust<>(t);
    }

    public <R> Observable<R> map(Function<T, R> function) {
        return new ObservableMap<>(this, function);
    }

    public Observable<T> doOnSubscribe(Consumer<? super Disposable> onSubscribe) {
        return new ObservableDoOnSubscribe<>(this, onSubscribe);
    }

    public Observable<T> doOnNext(Consumer<T> onNext) {
        return new ObservableDoOnEach<>(this, onNext, Functions.<Throwable>emptyConsumer(), Functions.EMPTY_ACTION, Functions.EMPTY_ACTION);
    }

    public Observable<T> doOnError(Consumer<? super Throwable> onError) {
        return new ObservableDoOnEach<>(this, Functions.<T>emptyConsumer(), onError, Functions.EMPTY_ACTION, Functions.EMPTY_ACTION);
    }

    public Observable<T> doOnComplete(Action onComplete) {
        return new ObservableDoOnEach<>(this, Functions.<T>emptyConsumer(), Functions.<Throwable>emptyConsumer(), onComplete, Functions.EMPTY_ACTION);
    }

    public Observable<T> doOnAfterTerminate(Action onAfterTerminate) {
        return new ObservableDoOnEach<>(this, Functions.<T>emptyConsumer(), Functions.<Throwable>emptyConsumer(), Functions.EMPTY_ACTION, onAfterTerminate);
    }

    public Observable<T> doOnDispose(Action onDispose) {
        return new ObservableDispose<>(this, onDispose);
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new ObservableSubscribeOn<>(this, scheduler);
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new ObservableObserveOn<>(this, scheduler);
    }

    public Observable<T> distinctUntilChanged() {
        return distinct(new Function<T, T>() {

            @Override
            public T apply(T t) throws IOException {
                return t;
            }
        });
    }

    public Observable<T> hide() {
        return new ObservableHide<>(this);
    }

    public Observable<T> takeUntil(Predicate<T> predicate) {
        return new ObservableTakeUntil<>(this, predicate);
    }

    public Observable<T> filter(Predicate<T> filter) {
        return new ObservableFilter<>(this, filter);
    }

    public <K> Observable<T> distinct(Function<T, K> func) {
        return new ObservableDistinct<>(this, func);
    }

    public static <T> Observable<T> fromCallable(Callable<T> callable) {
        return new ObservableFromCallable<>(callable);
    }

    @SafeVarargs
    public static <T> Observable<T> fromArray(T... array) {
        return new ObservableFromArray<>(array);
    }

    public Observable<T> elementAt(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("index >= 0 required but it was " + index);
        }
        return new ObservableElementAt<>(this, index);
    }

    public Observable<T> firstElement() {
        return elementAt(0);
    }

    public Observable<T> lastElement() {
        return new ObservableLastElement<>(this);
    }

    public Observable<T> take(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count >= 0 required but it was " + count);
        }
        return new ObservableTake<>(this, count);
    }

    public Observable<T> takeLast(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count >= 0 required but it was " + count);
        }
        return new ObservableTakeLast<>(this, count);
    }

    public <U> Observable<U> concatMap(Function<T, ObservableSource<U>> mapper) {
        return new ObservableConcatMap<>(this, mapper);
    }

    public <U> Observable<U> flatMap(Function<T, ObservableSource<U>> mapper) {
        return new ObservableFlatMap<>(this ,mapper);
    }

    public static Observable<Long> timer(long time, TimeUnit unit) {
        return new ObservableTimer(time, unit, Schedules.COMPUTATION);
    }

    @SafeVarargs
    public static <T> Observable<T> concat(Observable<T> ... observables) {
        return new ObservableFromArray<>(observables).concatMap(new Function<Observable<T>, ObservableSource<T>>() {
            @Override
            public ObservableSource<T> apply(Observable<T> observable) {
                return observable;
            }
        });
    }

    public static <T> Observable<T> wrap(ObservableSource<T> source) {
        if (source instanceof Observable) {
            return (Observable<T>)source;
        }
        return new ObservableFromUnsafeSource<>(source);
    }

    public final <R> Observable<R> compose(ObservableTransformer<T, R> upstream) {
        return wrap(upstream.apply(this));
    }

    @SafeVarargs
    public static <T> Observable<T> merge(Observable<T> ... observables) {
        return new ObservableFromArray<>(observables).flatMap(new Function<Observable<T>, ObservableSource<T>>() {
            @Override
            public ObservableSource<T> apply(Observable<T> observable) {
                return observable;
            }
        });
    }

    public static Observable<Long> interval(long period, TimeUnit unit) {
        return interval(0, period, unit, Schedules.COMPUTATION);
    }

    public static Observable<Long> interval(long initDelay, long period, TimeUnit unit, Scheduler scheduler) {
        return new ObservableInterval(initDelay, period, unit, scheduler);
    }

    /**
     * 会阻塞当前线程
     */
    public T blockingFirst() {
        BlockingFirstObserver<T> observer = new BlockingFirstObserver<>();
        subscribe(observer);
        return observer.blockingGet();
    }

    /**
     * 会阻塞当前线程
     */
    public T blockingLast() {
        BlockingLastObserver<T> observer = new BlockingLastObserver<>();
        subscribe(observer);
        return observer.blockingGet();
    }

    /**
     * blockingSubscribe操作符，会阻塞当前线程，直到subscribe结束
     */
    public void blockingSubscribe(Consumer<T> consumer) {
        LinkedBlockingDeque<Object> queue = new LinkedBlockingDeque<>();
        BlockingObserver<T> blockingObserver = new BlockingObserver<>(queue);
        subscribe(blockingObserver);    // 数据都已经存在了queue中
        for (;;) {
            Object v = queue.poll();
            if (v == null) {
                try {
                    v = queue.take(); // 这个会去获取锁，性能较差所以先通过poll，如果不为空就不需要take了
                } catch (InterruptedException e) {
                    blockingObserver.dispose();
                    RxJavaPlugins.onError(e);
                    return;
                }
            }
            if (blockingObserver.isDisposable() || v == BlockingObserver.TERMINATED) {
                break;
            }
            if (v == BlockingObserver.COMPLETE) {
                break;
            } else if (v instanceof Throwable) {
                break;
            }
            consumer.accept((T) v);
        }
    }

    public Disposable subscribe(Consumer<T> consumer) {
        LambdaObserver<T> ls = new LambdaObserver<>(consumer, Functions.<Throwable>emptyConsumer(), Functions.EMPTY_ACTION, Functions.EMPTY_ACTION);
        this.subscribe(ls);
        return ls;
    }

    public Disposable subscribe(Consumer<T> consumer, Consumer<Throwable> errConsumer) {
        LambdaObserver<T> ls = new LambdaObserver<>(consumer, errConsumer, Functions.EMPTY_ACTION, Functions.EMPTY_ACTION);
        this.subscribe(ls);
        return ls;
    }

    public Disposable subscribe(Consumer<T> consumer, Consumer<Throwable> errConsumer, Action completeAction) {
        LambdaObserver<T> ls = new LambdaObserver<>(consumer, errConsumer, completeAction, Functions.EMPTY_ACTION);
        this.subscribe(ls);
        return ls;
    }

    public Disposable subscribe(Consumer<T> consumer, Consumer<Throwable> errConsumer, Action completeAction, Action onSubscribeAction) {
        LambdaObserver<T> ls = new LambdaObserver<>(consumer, errConsumer, completeAction, onSubscribeAction);
        this.subscribe(ls);
        return ls;
    }

    public Disposable subscribe() {
        return subscribe(Functions.<T>emptyConsumer());
    }
}
