package com.android.demo.rxandroid.observable;

import java.util.LinkedList;
import java.util.Queue;

import com.android.demo.rxandroid.observer.BaseObserver;
import com.android.demo.rxandroid.observer.Observer;
import com.android.demo.rxandroid.schedule.Scheduler;

public class ObservableObserveOn<T> extends AbstractObservableWithUpStream<T, T>{

    private final Scheduler scheduler;

    public ObservableObserveOn(Observable<T> source, Scheduler scheduler) {
        super(source);
        this.scheduler = scheduler;
    }

    @Override
    public void subscribeActual(Observer<T> observer) {
        source.subscribe(new ObserveOnObserver<T>(observer, scheduler));
    }

    /**
     * 需要维持一个队列，否则onComplete会跑到onNext之前
     */
    private static class ObserveOnObserver<T> extends BaseObserver<T, T> implements Runnable {

        private final Scheduler scheduler;

        private final Queue<Runnable> queue = new LinkedList<>();

        public ObserveOnObserver(Observer<T> actual, Scheduler scheduler) {
            super(actual);
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(final T t) {
            queue.offer(new Runnable() {
                @Override
                public void run() {
                    actual.onNext(t);
                }
            });
            scheduler.scheduleDirect(this);
        }

        @Override
        public void dispose() {
            super.dispose();
            scheduler.scheduleDirect(this);
        }

        @Override
        public void onComplete() {
            queue.offer(new Runnable() {
                @Override
                public void run() {
                    actual.onComplete();
                }
            });
            scheduler.scheduleDirect(this);
        }

        @Override
        public void run() {
            while (!queue.isEmpty()) {
                synchronized (queue) {
                    Runnable poll = queue.poll();
                    poll.run();
                }
            }
        }
    }
}
