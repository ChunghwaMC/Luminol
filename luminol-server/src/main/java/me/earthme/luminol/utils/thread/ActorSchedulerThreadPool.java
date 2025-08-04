package me.earthme.luminol.utils.thread;

import ca.spottedleaf.concurrentutil.collection.MultiThreadedQueue;
import ca.spottedleaf.concurrentutil.util.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.TimeUtil;
import me.earthme.luminol.utils.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.VarHandle;
import java.util.Comparator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class ActorSchedulerThreadPool {
    public static final long DEADLINE_NOT_SET = Long.MIN_VALUE;

    private final ThreadFactory threadFactory;
    private final CopyOnWriteArrayList<SchedulerWorkerThreadCarrier> workers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<SchedulableTick, WorkerMessageNode> taskBandings = new ConcurrentHashMap<>();
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // TODO Balance them
    private final long minTaskDeadlineOffset = 1_000_000L; // 1ms
    private final long minTickTimeBuffer = 1000_000L; // 1ms

    private final long maxTaskDeadlineOffset = 5_000_000L; // 5ms
    private final long maxTickTimeBuffer = 5000_000L; // 5ms

    public ActorSchedulerThreadPool(int nThreads, ThreadFactory threadFactory, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.threadFactory = threadFactory;
        this.exceptionHandler = exceptionHandler;

        this.createNThreads(nThreads);
    }

    private void createNThreads(int nThreads) {
        for (int i = 0; i < nThreads; i++) {
            final SchedulerWorkerThreadCarrier createdWorker = new SchedulerWorkerThreadCarrier(this.threadFactory);

            this.workers.add(createdWorker);

            createdWorker.kickOff();
        }
    }

    public void shutdown() {
        if (!this.shutdown.compareAndSet(false, true)) {
            return;
        }

        for (SchedulerWorkerThreadCarrier worker : this.workers) {
            worker.killSignal();
        }

        // stop these threads from ticking
        for (WorkerMessageNode node : this.taskBandings.values()) {
            this.modifyValueOfTask(
                    node,
                    this.minTickTimeBuffer,
                    this.maxTaskDeadlineOffset,
                    true,
                    true
            );
        }
    }

    public Thread[] getThreads() {
        final SchedulerWorkerThreadCarrier[] workers = this.workers.toArray(new SchedulerWorkerThreadCarrier[0]);

        final Thread[] ret = new Thread[workers.length];

        for (int i = 0; i < workers.length; i++) {
            ret[i] = workers[i].runner;
        }

        return ret;
    }

    public boolean awaitTermination(long time, @NotNull TimeUnit unit) {
        long countDown = unit.toNanos(time);

        while (true) {
            if (!this.anyThreadAlive()) {
                return true;
            }

            if (countDown <= 0) {
                return false;
            }

            countDown -= 100;

            Thread.yield();
            LockSupport.parkNanos(100);
        }
    }

    private boolean anyThreadAlive() {
        for (SchedulerWorkerThreadCarrier worker : this.workers) {
            if (worker.status.get() != SchedulerWorkerThreadCarrier.STATUS_SHUTDOWN) {
                return true;
            }
        }

        return false;
    }

    private void modifyValueOfTask(
            WorkerMessageNode target,
            long tickDeadlineOffset,
            long mainThreadTaskPeriod,
            boolean interruptMainThreadTask,
            boolean pushTickWithinMinTickDeadlineBuffer
    ) {
        final Consumer<WorkerMessageNode> action = node -> {
            target.tickTimeDeadlineBuffer = tickDeadlineOffset;
            target.tickTimeDeadlineBuffer = Math.max(this.minTickTimeBuffer, target.tickTimeDeadlineBuffer);
            target.tickTimeDeadlineBuffer = Math.min(this.maxTickTimeBuffer, target.tickTimeDeadlineBuffer);

            target.mainThreadTaskPeriod = mainThreadTaskPeriod;
            target.mainThreadTaskPeriod = Math.max(this.minTaskDeadlineOffset, target.mainThreadTaskPeriod);
            target.mainThreadTaskPeriod = Math.min(this.maxTaskDeadlineOffset, target.mainThreadTaskPeriod);

            // we only do this when we are processed inside a large message block(WorkerMessageNode)
            if (node != null) {
                target.mainThreadTaskInterrupted = interruptMainThreadTask;
                target.pushTickWithinMinTickDeadlineBuffer = pushTickWithinMinTickDeadlineBuffer;
            }
        };

        final SubMessageNode wrappedAction = new SubMessageNode(action, target, () -> action.accept(null));

        if (target.sendMessage(wrappedAction)) {
            target.notifyReceiver();
        }
    }

    private @Nullable ActorSchedulerThreadPool.SchedulerWorkerThreadCarrier selectWorker() {
        // idle first
        for (SchedulerWorkerThreadCarrier w : workers) {
            if (w.status.get() == SchedulerWorkerThreadCarrier.STATUS_IDLE) {
                return w;
            }
        }

        // idle first failed, chose one randomly
        int r = ThreadLocalRandom.current().nextInt(workers.size());
        for (int i = 0; i < workers.size(); i++) {
            SchedulerWorkerThreadCarrier w = workers.get((r + i) % workers.size());
            if (w.status.get() != SchedulerWorkerThreadCarrier.STATUS_SHUTDOWN) {
                return w;
            }
        }

        return null;
    }

    private void calculateTimeBuffer(@NotNull WorkerMessageNode target) {
         final long deadlineApproachAvg = Math.max(target.getLastDeadlineApproachAvg(), 0);
         final long tickExecutionAvg = target.getAvgExecutionTime();

         if (tickExecutionAvg == 0 || deadlineApproachAvg == 0) {
             // not fully initialized yet
             return;
         }

         final double load = (double) tickExecutionAvg / ((double) deadlineApproachAvg + (double) tickExecutionAvg);

        // recalculate time buffer
        final long currToApproach_tickDeadlineOffset = this.minTickTimeBuffer + (long) load * (this.maxTickTimeBuffer - this.minTickTimeBuffer);
        final long currToApproach_taskDeadlineSingle = this.minTaskDeadlineOffset + (long) (((this.maxTaskDeadlineOffset - this.minTaskDeadlineOffset)) * (1 - load));

        this.modifyValueOfTask(
                target,
                currToApproach_tickDeadlineOffset,
                currToApproach_taskDeadlineSingle,
                false,
                true // Interrupt tick once (we'll process the tick soon later)
        );
    }

    public void notifyTask(SchedulableTick task) {
        final WorkerMessageNode target = this.taskBandings.get(task);

        if (target == null) {
            return;
        }

        this.calculateTimeBuffer(target);
        this.dispatchMessageNodeAuto(target, true);
    }

    public void schedule(SchedulableTick task) {
        final WorkerMessageNode created = new WorkerMessageNode(task);

        this.taskBandings.put(task, created);

        this.dispatchMessageNodeAuto(created, false);
    }

    private void removeMessageNode(@NotNull WorkerMessageNode messageNode) {
        this.taskBandings.remove(messageNode.internal);
    }

    private void dispatchMessageNodeAuto(@NotNull WorkerMessageNode workerMessageNode, boolean insideDispatcherContextOrCall) {
        final SchedulerWorkerThreadCarrier targetWorker = this.selectWorker();

        if (targetWorker == null) { // no threads available, might be shut down
            if (insideDispatcherContextOrCall) {
                return;
            }

            throw new RejectedExecutionException("shutdown");
        }

        // already dispatched by other
        if (!workerMessageNode.tryPreDispatch(targetWorker)) {
            // probably already scheduled to target
            final SchedulerWorkerThreadCarrier currBelongTo = workerMessageNode.getOwnerWorker();

            if (currBelongTo != null) {
                currBelongTo.notifyWorker();
            }

            return;
        }

        this.calculateTimeBuffer(workerMessageNode);
        if (!targetWorker.message(workerMessageNode) && !this.shutdown.get()) {
            workerMessageNode.cleanOwnerWorker(); // we need to reset this to prevent task losing from queue

            this.dispatchMessageNodeAuto(workerMessageNode, true);
        }
    }

    // copied from concurrentutil
    public static abstract class SchedulableTick {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        public final long id = ID_GENERATOR.getAndIncrement();
        private long scheduledStart = DEADLINE_NOT_SET;

        public final long getScheduledStart() {
            return this.scheduledStart;
        }

        public final void setScheduledStart(final long value) {
            this.scheduledStart = value;
        }

        public abstract boolean runTick();

        public abstract boolean hasTasks();

        public abstract boolean runTasks(final BooleanSupplier canContinue);

        @Override
        public String toString() {
            return "SchedulableTick:{" +
                    "class=" + this.getClass().getName() + ","
                    + "}";
        }
    }

    private final class SubMessageNode {
        private final Consumer<WorkerMessageNode> action;
        @Nullable
        private final WorkerMessageNode insideTask;
        private final Runnable ifFinalized;

        private SubMessageNode(
                Consumer<WorkerMessageNode> action,
                @Nullable WorkerMessageNode insideTask,
                Runnable ifFinalized
        ) {
            this.action = action;
            this.insideTask = insideTask;
            this.ifFinalized = ifFinalized;
        }

        public void process() {
            try {
                this.action.accept(this.insideTask);
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(Thread.currentThread(), ex);
            }
        }

        public void doFinalized() {
            try {
                if (this.ifFinalized != null) {
                    this.ifFinalized.run();
                }
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(Thread.currentThread(), ex);
            }
        }
    }

    private final class WorkerMessageNode {
        private final SchedulableTick internal;
        private final MultiThreadedQueue<SubMessageNode> subMessageNodes = new MultiThreadedQueue<>();

        private long tickTimeDeadlineBuffer = ActorSchedulerThreadPool.this.maxTickTimeBuffer;
        private long mainThreadTaskPeriod = ActorSchedulerThreadPool.this.maxTaskDeadlineOffset;

        private SchedulerWorkerThreadCarrier ownerWorker;

        private boolean wannaReinsert = true;
        private boolean executed = false;

        private boolean mainThreadTaskInterrupted = false;
        private boolean pushTickWithinMinTickDeadlineBuffer = false;

        private final AtomicLong lastTickExecutionTimeSum = new AtomicLong();
        private final AtomicLong executedTickCount = new AtomicLong();

        private final AtomicLong lastDeadlineApproachSum = new AtomicLong();
        private final AtomicInteger passedTimes = new AtomicInteger();

        private static final VarHandle OWNER_HANDLE = ConcurrentUtil.getVarHandle(WorkerMessageNode.class, "ownerWorker", SchedulerWorkerThreadCarrier.class);

        private WorkerMessageNode(SchedulableTick internal) {
            this.internal = internal;
        }

        public boolean tryPreDispatch(SchedulerWorkerThreadCarrier carrier) {
            return this.trySetWorker(carrier);
        }

        public void setWorker(SchedulerWorkerThreadCarrier carrier) {
            OWNER_HANDLE.setVolatile(this, carrier);
        }

        public boolean trySetWorker(SchedulerWorkerThreadCarrier ownerWorker) {
            return OWNER_HANDLE.compareAndSet(this, null, ownerWorker);
        }

        private void cleanOwnerWorker() {
            OWNER_HANDLE.setVolatile(this, null);
        }

        private SchedulerWorkerThreadCarrier getOwnerWorker() {
            return (SchedulerWorkerThreadCarrier) OWNER_HANDLE.getVolatile(this);
        }

        public void notifyReceiver() {
            final SchedulerWorkerThreadCarrier owner = (SchedulerWorkerThreadCarrier) OWNER_HANDLE.getVolatile(this);

            if (owner == null) {
                return;
            }

            LockSupport.unpark(owner.runner);
        }

        private void resetContextFlags() {
            this.mainThreadTaskInterrupted = false;
            this.pushTickWithinMinTickDeadlineBuffer = false;
        }

        public boolean sendMessage(SubMessageNode subMessageNode) {
            return this.subMessageNodes.offer(subMessageNode);
        }

        public void doMessageProcess() {
            boolean canceled = false;

            try {
                final long tickDeadline = this.internal.getScheduledStart();
                final long taskDeadline = System.nanoTime() + this.mainThreadTaskPeriod;
                final AtomicInteger executedCount = new AtomicInteger(0);

                this.lastDeadlineApproachSum.getAndAdd(tickDeadline - System.nanoTime());
                this.passedTimes.getAndIncrement();

                if (this.internal.hasTasks()) {
                    canceled = !this.internal.runTasks(() -> {
                        this.processSubMessageNode();

                        final long remaining = System.nanoTime() - taskDeadline;

                        executedCount.incrementAndGet();
                        return remaining > 0 && !this.mainThreadTaskInterrupted;
                    });
                }

                this.executed = executedCount.get() > 0;

                if (canceled) {
                    return;
                }

                long remaining = System.nanoTime() - tickDeadline;

                // we have enough time for this tick, so reinsert back for load balance
                if ((remaining + this.tickTimeDeadlineBuffer) < 0) {
                    return;
                }

                // we pushed tick for more task runs
                if (this.pushTickWithinMinTickDeadlineBuffer && (remaining + ActorSchedulerThreadPool.this.minTickTimeBuffer) < 0) {
                    return;
                }

                for (;;) {
                    this.processSubMessageNode();

                    remaining = System.nanoTime() - tickDeadline;

                    // might someone notified for a task execution within its min time buffer
                    if (this.pushTickWithinMinTickDeadlineBuffer && (remaining + ActorSchedulerThreadPool.this.minTickTimeBuffer) < 0) {
                        return;
                    }

                    if (remaining < 0) {
                        if (this.internal.hasTasks()) {
                            // we would gonna process more tasks during waiting
                            canceled = !this.internal.runTasks(() -> {
                                this.processSubMessageNode();

                                final long remainingCurr = System.nanoTime() - tickDeadline;

                                return remainingCurr > 0 && !this.mainThreadTaskInterrupted;
                            });

                            if (canceled) {
                                break;
                            }

                            continue;
                        }

                        Thread.yield();
                        LockSupport.parkNanos("AWAIT DEADLINE", 1_000L);
                        continue;
                    }

                    final long currTime = System.nanoTime();

                    canceled = !this.internal.runTick();

                    this.executedTickCount.getAndIncrement();
                    this.lastTickExecutionTimeSum.getAndAdd(System.nanoTime() - currTime);
                    this.executed = true;
                    break;
                }
            }finally {
                this.finalizeSubMsgBelongToSelf(false);
                this.resetContextFlags();

                this.wannaReinsert = !canceled;
                this.cleanOwnerWorker();
            }
        }

        private long getAvgExecutionTime() {
            final long executed = this.executedTickCount.get();
            final long sum = this.lastTickExecutionTimeSum.get();

            if (executed == 0) {
                return 0; // we will process this value
            }

            return sum / executed;
        }

        private long getLastDeadlineApproachAvg() {
            final long lastDeadlineApproachSum = this.lastDeadlineApproachSum.get();
            final long passedTime = this.passedTimes.get();

            if (passedTime == 0) {
                return 0; // we will process this value
            }

            return lastDeadlineApproachSum / passedTime;
        }

        private void processSubMessageNode() {
            final SubMessageNode subMessageNode = this.subMessageNodes.poll();

            // might have a interrupt message incoming
            if (subMessageNode != null) {
                subMessageNode.process();
            }
        }

        public void finalizeSubMsgBelongToSelf(boolean canceled) {
            SubMessageNode subMessageNode;
            while ((subMessageNode = canceled ? this.subMessageNodes.pollOrBlockAdds() : this.subMessageNodes.poll()) != null) {
                try {
                    subMessageNode.doFinalized();
                }catch (Exception ex) {
                    final SchedulerWorkerThreadCarrier owner = this.getOwnerWorker();

                    ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(owner != null ? owner.runner : null, ex);
                }
            }
        }

        public void onCancelled() {
            this.finalizeSubMsgBelongToSelf(true);
        }
    }

    // use this to implement the "pollOrBlockAdd function like MultiThreadedQueue"
    private static final class SchedulerWorkerQueueConditioner {
        private int referenceCount = 0;
        private boolean addBlocked = false;

        private static final VarHandle REFERENCE_COUNT_HANDLE = ConcurrentUtil.getVarHandle(SchedulerWorkerQueueConditioner.class, "referenceCount", int.class);
        private static final VarHandle BLOCK_ADD_HANDLE = ConcurrentUtil.getVarHandle(SchedulerWorkerQueueConditioner.class, "addBlocked", boolean.class);
        
        private boolean isAddBlocked() {
            return (boolean) BLOCK_ADD_HANDLE.getVolatile(this);
        }

        private void blockAdd() {
            BLOCK_ADD_HANDLE.setVolatile(this, true);
        }

        private void releaseWriteReference() {
            if (!REFERENCE_COUNT_HANDLE.compareAndSet(this, -1, 0)) {
                throw new IllegalStateException("Releasing when not write-locked");
            }
        }

        private void acquireWriteReference() {
            int failureCount = 0;
            for (;;) {
                for (int i = 0; i < failureCount; i++) {
                    ConcurrentUtil.backoff();
                }

                final int curr = (int) REFERENCE_COUNT_HANDLE.getVolatile(this);

                if (curr > 0 || curr == -1) {
                    failureCount++;
                    continue;
                }

                if (!REFERENCE_COUNT_HANDLE.compareAndSet(this, curr, -1)) {
                    failureCount++;
                    continue;
                }

                break;
            }
        }

        private void releaseReadReference() {
            int failureCount = 0;
            for (;;) {
                for (int i = 0; i < failureCount; i++) {
                    ConcurrentUtil.backoff();
                }

                final int curr = (int) REFERENCE_COUNT_HANDLE.getVolatile(this);

                if (curr == -1) {
                    throw new IllegalStateException("Cannot release read reference when write locked");
                }

                if (curr == 0) {
                    throw new IllegalStateException("Setting reference count down to a value lower than 0!");
                }

                if (!REFERENCE_COUNT_HANDLE.compareAndSet(this, curr, curr - 1)) {
                    failureCount++;
                    continue;
                }

                break;
            }
        }

        private void acquireReadReference() {
            int failureCount = 0;
            for (;;) {
                for (int i = 0; i < failureCount; i++) {
                    ConcurrentUtil.backoff();
                }

                final int curr = (int) REFERENCE_COUNT_HANDLE.getVolatile(this);

                if (curr == -1) {
                    failureCount++;
                    continue;
                }

                if (!REFERENCE_COUNT_HANDLE.compareAndSet(this, curr, curr + 1)) {
                    failureCount++;
                    continue;
                }

                break;
            }
        }
    }

    private final class SchedulerWorkerThreadCarrier implements Runnable {
        private static final Comparator<WorkerMessageNode> TICK_COMPARATOR_BY_TIME = (t1, t2) -> {
            int timeCompare = TimeUtil.compareTimes(t1.internal.scheduledStart, t2.internal.scheduledStart);
            return timeCompare != 0 ? timeCompare : Long.compare(t1.internal.id, t2.internal.id);
        };

        public static final int STATUS_IDLE = 0;
        public static final int STATUS_SHUTDOWN = 1;
        public static final int STATUS_BUSY = 2;
        public static final int STATUS_RUNNING = 3;

        private final Thread runner;
        private final ConcurrentSkipListSet<WorkerMessageNode> inComingTaskMessages = new ConcurrentSkipListSet<>(TICK_COMPARATOR_BY_TIME);
        private final SchedulerWorkerQueueConditioner queueConditioner = new SchedulerWorkerQueueConditioner();

        private final AtomicBoolean killSignal = new AtomicBoolean(false);
        private final AtomicInteger status = new AtomicInteger(0);

        private final AtomicInteger validTaskExecutedCnt = new AtomicInteger(0);
        private final AtomicInteger loopedTimes = new AtomicInteger(0);

        private SchedulerWorkerThreadCarrier(@NotNull ThreadFactory factory) {
            runner = factory.newThread(this);
        }

        public void kickOff() {
            this.status.set(STATUS_RUNNING);
            this.runner.start();
        }

        @Override
        public void run() {
            int executeFailureCount = 0;
            for (;;) {
                this.loopedTimes.getAndIncrement();

                final boolean killed = this.killSignal.get();
                WorkerMessageNode incomingMessage = this.takeMessage(killed);

                // no more task stay in curr thread and we were killed
                if (killed && incomingMessage == null) {
                    break;
                }

                if (incomingMessage != null) {
                    this.status.set(STATUS_BUSY);

                    Pair<Boolean, Boolean> result = this.processMessage(incomingMessage);

                    final boolean wannaReinsert = result.left();
                    final boolean executed = result.right();

                    if (wannaReinsert) {
                        ActorSchedulerThreadPool.this.dispatchMessageNodeAuto(incomingMessage, true);
                    }else {
                        incomingMessage.onCancelled();

                        ActorSchedulerThreadPool.this.removeMessageNode(incomingMessage);
                    }

                    if (executed) {
                        executeFailureCount = 0;
                        this.validTaskExecutedCnt.incrementAndGet();
                        continue;
                    }
                    executeFailureCount++;
                } else {
                    // steal some task from other busy threads
                    // here we won't increase the executeFailed cnt when steal failed as we are not running tasks of ourselves
                    SchedulerWorkerThreadCarrier other = this.randomSelect();
                    final int maxStealAttempts = ActorSchedulerThreadPool.this.workers.size();

                    for (int i = 0; i < maxStealAttempts && !(other != null && other != this); i++) {
                        other = this.randomSelect();
                    }

                    if (other != null && other != this) {
                        incomingMessage = other.steal();
                    }

                    if (incomingMessage != null) {
                        // it is in our queue now
                        incomingMessage.setWorker(this);

                        this.inComingTaskMessages.add(incomingMessage);
                        continue;
                    }
                }

                this.status.set(STATUS_IDLE);

                executeFailureCount++;

                // sleep 1 - 100us
                LockSupport.parkNanos("IDLE", Math.max(Math.max(executeFailureCount, 1), 100) * 1000L);
            }

            this.status.set(STATUS_SHUTDOWN);
        }

        @Contract("_ -> new")
        private @NotNull Pair<Boolean, Boolean> processMessage(@NotNull WorkerMessageNode node) {

            try {
                node.doMessageProcess();
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(this.runner, ex);
            }

            return Pair.of(node.wannaReinsert, node.executed);
        }

        private WorkerMessageNode takeMessage(boolean blockAdd) {
            if (blockAdd) {
                this.queueConditioner.acquireWriteReference();
                this.queueConditioner.blockAdd();
                this.queueConditioner.releaseWriteReference();
            }

            return this.inComingTaskMessages.pollFirst();
        }

        private void killSignal() {
            if (this.killSignal.compareAndSet(false, true)) {
                LockSupport.unpark(this.runner);
            }
        }

        private WorkerMessageNode steal() {
            return this.inComingTaskMessages.pollFirst();
        }

        private boolean message(WorkerMessageNode messageNode) {
            if (this.killSignal.get()) {
                return false;
            }

            boolean queued = false;

            this.queueConditioner.acquireReadReference();

            if (!this.queueConditioner.isAddBlocked()) {
                queued = this.inComingTaskMessages.add(messageNode);
            }

            this.queueConditioner.releaseReadReference();

            if (queued) {
                LockSupport.unpark(this.runner);
            }

            return queued;
        }

        private void notifyWorker() {
            LockSupport.unpark(this.runner);
        }

        private SchedulerWorkerThreadCarrier randomSelect() {
            final SchedulerWorkerThreadCarrier[] allSchedulers = ActorSchedulerThreadPool.this.workers.toArray(new SchedulerWorkerThreadCarrier[0]);
            final ThreadLocalRandom random = ThreadLocalRandom.current();

            return allSchedulers[random.nextInt(allSchedulers.length)];
        }
    }
}
