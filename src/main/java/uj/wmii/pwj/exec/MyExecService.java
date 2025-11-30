package uj.wmii.pwj.exec;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MyExecService implements ExecutorService {

    private final Deque<Runnable> workQueue = new LinkedList<>();
    private final Thread workerThread;

    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;

    private final Object terminationLock = new Object();

    static MyExecService newInstance() {
        return new MyExecService();
    }

    private MyExecService() {
        this.workerThread = new Thread(this::workerLoop, "MyExec-Worker");
        this.workerThread.start();
    }

    private void workerLoop() {
        while (true) {
            Runnable task;
            synchronized (workQueue) {
                while (workQueue.isEmpty() && !isShutdown) {
                    try {
                        workQueue.wait();
                    } catch (InterruptedException e) {
                        if (isShutdown) break;
                    }
                }

                if (isShutdown && workQueue.isEmpty()) {
                    break;
                }

                task = workQueue.pollFirst();
            }

            if (task != null) {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                }
            }
        }

        isTerminated = true;
        synchronized (terminationLock) {
            terminationLock.notifyAll();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        synchronized (workQueue) {
            if (isShutdown) {
                throw new RejectedExecutionException("Executor is shut down");
            }
            workQueue.addLast(command);
            workQueue.notify();
        }
    }

    @Override
    public void shutdown() {
        synchronized (workQueue) {
            isShutdown = true;
            workQueue.notifyAll();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> pendingTasks;
        synchronized (workQueue) {
            isShutdown = true;
            pendingTasks = new ArrayList<>(workQueue);
            workQueue.clear();
            workQueue.notifyAll();
        }
        workerThread.interrupt();
        return pendingTasks;
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanos;

        synchronized (terminationLock) {
            while (!isTerminated) {
                if (nanos <= 0) {
                    return false;
                }
                long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
                int remainder = (int) (nanos % 1_000_000);
                if (millis == 0 && remainder > 0) millis = 1;

                terminationLock.wait(millis, remainder);
                nanos = deadline - System.nanoTime();
            }
            return true;
        }
    }


    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<T> ftask = new FutureTask<>(task);
        execute(ftask);
        return ftask;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<T> ftask = new FutureTask<>(task, result);
        execute(ftask);
        return ftask;
    }

    @Override
    public Future<?> submit(Runnable task) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<Void> ftask = new FutureTask<>(task, null);
        execute(ftask);
        return ftask;
    }


    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        if (tasks == null) throw new NullPointerException();
        List<Future<T>> futures = new ArrayList<>(tasks.size());

        for (Callable<T> t : tasks) {
            RunnableFuture<T> f = new FutureTask<>(t);
            futures.add(f);
            execute(f);
        }

        for (Future<T> f : futures) {
            try {
                if (!f.isDone()) f.get();
            } catch (ExecutionException | CancellationException ignore) {
            }
        }
        return futures;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        if (tasks == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanos;

        List<Future<T>> futures = new ArrayList<>(tasks.size());

        for (Callable<T> t : tasks) {
            futures.add(new FutureTask<>(t));
        }

        for (Future<T> f : futures) {
            execute((Runnable) f);
        }

        for (Future<T> f : futures) {
            if (!f.isDone()) {
                if (nanos <= 0L) {
                    return cancelAll(futures);
                }
                try {
                    f.get(nanos, TimeUnit.NANOSECONDS);
                } catch (CancellationException | ExecutionException ignore) {
                } catch (TimeoutException toe) {
                    return cancelAll(futures);
                }
                long now = System.nanoTime();
                nanos = deadline - now;
            }
        }
        return futures;
    }

    private <T> List<Future<T>> cancelAll(List<Future<T>> futures) {
        for (Future<T> f : futures) {
            f.cancel(true);
        }
        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        try {
            return invokeAny(tasks, 0, null);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (tasks == null || tasks.isEmpty()) throw new IllegalArgumentException();

        List<CompletableFuture<T>> cfs = new ArrayList<>();

        for (Callable<T> task : tasks) {
            // SupplyAsync domyślnie używa ForkJoinPool, musimy użyć naszego executora (this)
            CompletableFuture<T> cf = CompletableFuture.supplyAsync(() -> {
                try {
                    return task.call();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, this);
            cfs.add(cf);
        }


        BlockingQueue<Future<T>> completionQueue = new LinkedBlockingQueue<>();
        List<Future<T>> futures = new ArrayList<>();

        for (Callable<T> t : tasks) {
            FutureTask<T> f = new FutureTask<T>(t) {
                @Override
                protected void done() {
                    completionQueue.add(this);
                }
            };
            futures.add(f);
            execute(f);
        }

        int active = futures.size();
        List<ExecutionException> errors = new ArrayList<>();
        long nanos = (unit != null) ? unit.toNanos(timeout) : Long.MAX_VALUE;
        long deadline = System.nanoTime() + nanos;

        try {
            while (active > 0) {
                Future<T> f;
                if (unit == null) {
                    f = completionQueue.take();
                } else {
                    nanos = deadline - System.nanoTime();
                    if (nanos <= 0) throw new TimeoutException();
                    f = completionQueue.poll(nanos, TimeUnit.NANOSECONDS);
                    if (f == null) throw new TimeoutException();
                }

                try {
                    return f.get();
                } catch (ExecutionException ee) {
                    errors.add(ee);
                    active--;
                }
            }
            ExecutionException e = new ExecutionException("No task completed successfully", new Exception());
            for(ExecutionException ee : errors) e.addSuppressed(ee);
            throw e;

        } finally {
            for (Future<T> f : futures) f.cancel(true);
        }
    }
}