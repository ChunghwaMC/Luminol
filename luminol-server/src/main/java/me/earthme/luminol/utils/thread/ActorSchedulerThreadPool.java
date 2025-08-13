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

/**
 * Used for periodic and long term tick-loop dispatch
 * Support mid-tasks(execute main thread tasks during the wait of deadline)
 * Tasks are spreading across workers freely and frequently to support run mid-tasks during the waiting
 * And there is also a task stealing logic to prevent too high load on a single worker
 * Inspired by ForkJoinPool and SchedulerThreadPool
 * @code ForkJoinPool
 * @code ca.spottedleaf.concurrentutil.scheduler.SchedulerThreadPool
 */
public class ActorSchedulerThreadPool {
    public static final long DEADLINE_NOT_SET = Long.MIN_VALUE;

    private final ThreadFactory threadFactory;
    private final CopyOnWriteArrayList<SchedulerWorkerCarrier> workers = new CopyOnWriteArrayList<>();
    // used for task dispatch
    private final MultiThreadedQueue<SchedulerWorkerCarrier> idleWorkers = new MultiThreadedQueue<>();
    // used for task stats notification
    private final ConcurrentHashMap<SchedulableTick, WorkerTaskNode> taskBandings = new ConcurrentHashMap<>();
    private final Thread.UncaughtExceptionHandler exceptionHandler;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // Use default settings as what folia's new scheduler does (minimal task deadline time)
    private final SchedulerConfig schedulerConfig = new SchedulerConfig(
            // task deadline offset control
            TimeUnit.MILLISECONDS.toNanos(2), // 2ms
            TimeUnit.MILLISECONDS.toNanos(4), // 4ms
            // tick deadline offset control
            TimeUnit.MILLISECONDS.toNanos(4), // 4ms
            TimeUnit.MILLISECONDS.toNanos(6) // 4ms
    );

    public ActorSchedulerThreadPool(int nThreads, ThreadFactory threadFactory, Thread.UncaughtExceptionHandler exceptionHandler) {
        this.threadFactory = threadFactory;
        this.exceptionHandler = exceptionHandler;

        this.createNThreads(nThreads);
    }

    private void createNThreads(int nThreads) {
        for (int i = 0; i < nThreads; i++) {
            final SchedulerWorkerCarrier createdWorker = new SchedulerWorkerCarrier(this.threadFactory);

            this.workers.add(createdWorker);

            createdWorker.kickOff();
        }
    }

    /**
     * Shutdown the thread pool
     */
    public void shutdown() {
        if (!this.shutdown.compareAndSet(false, true)) {
            return;
        }

        for (SchedulerWorkerCarrier worker : this.workers) {
            worker.killSignal();
        }

        // stop these threads from ticking
        for (WorkerTaskNode node : this.taskBandings.values()) {
            this.modifyValueOfTask(
                    node,
                    this.schedulerConfig.minTickTimeWaitBuffer,
                    this.schedulerConfig.maxTickTimeWaitBuffer,
                    true, // soft interruption
                    true, // soft interruption
                    true // unpark to force worker process this msg
            );
        }
    }

    /**
     * Get all working threads
     * @return the copy of thread array
     */
    public Thread[] getThreads() {
        final SchedulerWorkerCarrier[] workers = this.workers.toArray(new SchedulerWorkerCarrier[0]);

        final Thread[] ret = new Thread[workers.length];

        for (int i = 0; i < workers.length; i++) {
            ret[i] = workers[i].runner;
        }

        return ret;
    }

    /**
     * Await all threads termination
     * @param time max wait time
     * @param unit the unit of wait time
     * @return true if all workers terminated
     */
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
        for (SchedulerWorkerCarrier worker : this.workers) {
            if (worker.status.get() != SchedulerWorkerCarrier.STATUS_SHUTDOWN) {
                return true;
            }
        }

        return false;
    }

    private void modifyValueOfTask(
            WorkerTaskNode target,
            long tickDeadlineOffset,
            long mainThreadTaskPeriod,
            boolean interruptMainThreadTask,
            boolean pushTickWithinMinTickDeadlineBuffer,
            boolean signal
    ) {
        final Consumer<WorkerTaskNode> action = node -> {
            target.tickTimeDeadlineBuffer = tickDeadlineOffset;
            target.tickTimeDeadlineBuffer = Math.max(this.schedulerConfig.minTickTimeWaitBuffer, target.tickTimeDeadlineBuffer);
            target.tickTimeDeadlineBuffer = Math.min(this.schedulerConfig.maxTickTimeWaitBuffer, target.tickTimeDeadlineBuffer);

            target.mainThreadTaskPeriod = mainThreadTaskPeriod;
            target.mainThreadTaskPeriod = Math.max(this.schedulerConfig.minTaskDeadlineBuffer, target.mainThreadTaskPeriod);
            target.mainThreadTaskPeriod = Math.min(this.schedulerConfig.maxTaskDeadlineBuffer, target.mainThreadTaskPeriod);

            // we only do this when we are processed inside a large message block(WorkerMessageNode)
            if (node != null) {
                target.mainThreadTaskInterrupted = interruptMainThreadTask;
                target.pushTickWithinMinTickDeadlineBuffer = pushTickWithinMinTickDeadlineBuffer;
            }
        };

        final InternalMessage wrappedAction = new InternalMessage(action, target, () -> action.accept(null));

        if (target.sendMessage(wrappedAction)) {
            if (signal) {
                target.notifyReceiver();
            }
        }
    }

    private @Nullable ActorSchedulerThreadPool.SchedulerWorkerCarrier selectWorker() {
        // try getting an idle worker immediately
        final SchedulerWorkerCarrier idleFirst = this.idleWorkers.poll();
        if (idleFirst != null) {
            return idleFirst;
        }

        // Check for workers with the lowest load (fewest tasks)
        SchedulerWorkerCarrier bestWorker = null;
        int minTaskCount = Integer.MAX_VALUE;
        
        for (SchedulerWorkerCarrier w : workers) {
            if (w.status.get() != SchedulerWorkerCarrier.STATUS_SHUTDOWN) {
                int taskCount = w.inComingTaskMessages.size();
                if (taskCount < minTaskCount) {
                    minTaskCount = taskCount;
                    bestWorker = w;
                }
            }
        }
        
        if (bestWorker != null) {
            return bestWorker;
        }

        // idle first and load low first failed, chose one randomly
        int r = ThreadLocalRandom.current().nextInt(workers.size());
        for (int i = 0; i < workers.size(); i++) {
            SchedulerWorkerCarrier w = workers.get((r + i) % workers.size());
            if (w.status.get() != SchedulerWorkerCarrier.STATUS_SHUTDOWN) {
                return w;
            }
        }

        return null;
    }

    private void calculateTimeBuffer(@NotNull ActorSchedulerThreadPool.WorkerTaskNode target, boolean signal) {
         final long deadlineApproachAvg = Math.max(target.getLastDeadlineApproachAvg(), 0);
         final long tickExecutionAvg = target.getAvgExecutionTime();

         if (tickExecutionAvg == 0 || deadlineApproachAvg == 0) {
             // not fully initialized yet
             return;
         }

         final double load = (double) tickExecutionAvg / ((double) deadlineApproachAvg + (double) tickExecutionAvg);

        // recalculate time buffer
        final long currToApproach_tickDeadlineOffset = this.schedulerConfig.minTickTimeWaitBuffer + (long) load * (this.schedulerConfig.maxTickTimeWaitBuffer - this.schedulerConfig.minTickTimeWaitBuffer);
        final long currToApproach_taskDeadlineSingle = this.schedulerConfig.minTaskDeadlineBuffer + (long) (((this.schedulerConfig.maxTaskDeadlineBuffer - this.schedulerConfig.minTaskDeadlineBuffer)) * (1 - load));

        this.modifyValueOfTask(
                target,
                currToApproach_tickDeadlineOffset,
                currToApproach_taskDeadlineSingle,
                false,
                true, // Interrupt tick once (we'll process the tick soon later)
                signal
        );
    }

    /**
     * Try push a scheduled task to run its tasks
     * @param task target task
     */
    public void notifyTask(SchedulableTick task) {
        final WorkerTaskNode target = this.taskBandings.get(task);

        if (target == null) {
            return;
        }

        this.dispatchMessageNodeAuto(target, true);
    }

    /**
     * Schedule a task into this pool
     * @param task the task will be scheduled
     */
    public void schedule(SchedulableTick task) {
        final WorkerTaskNode created = new WorkerTaskNode(task);

        this.taskBandings.put(task, created);

        this.dispatchMessageNodeAuto(created, false);
    }

    private void removeMessageNode(@NotNull ActorSchedulerThreadPool.WorkerTaskNode messageNode) {
        this.taskBandings.remove(messageNode.internal);
    }

    private void dispatchMessageNodeAuto(@NotNull ActorSchedulerThreadPool.WorkerTaskNode workerTaskNode, boolean insideDispatcherContextOrCall) {
        final SchedulerWorkerCarrier targetWorker = this.selectWorker();

        if (targetWorker == null) { // no threads available, might be shut down
            if (insideDispatcherContextOrCall) {
                return;
            }

            throw new RejectedExecutionException("shutdown");
        }

        // already dispatched by other
        if (!workerTaskNode.tryPreDispatch(targetWorker)) {
            // probably already scheduled to target
            final SchedulerWorkerCarrier currBelongTo = workerTaskNode.getOwnerWorker();

            if (currBelongTo != null) {
                currBelongTo.notifyWorker();
            }

            return;
        }

        this.calculateTimeBuffer(workerTaskNode, false);
        if (!targetWorker.message(workerTaskNode) && !this.shutdown.get()) {
            workerTaskNode.cleanOwnerWorker(); // we need to reset this to prevent task losing from queue

            this.dispatchMessageNodeAuto(workerTaskNode, true);
        }
    }

    private record SchedulerConfig(long minTaskDeadlineBuffer, long maxTaskDeadlineBuffer, long minTickTimeWaitBuffer,
                                   long maxTickTimeWaitBuffer) {
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

    private final class InternalMessage {
        private final Consumer<WorkerTaskNode> action;
        @Nullable
        private final ActorSchedulerThreadPool.WorkerTaskNode insideTask;
        private final Runnable ifFinalized;

        private InternalMessage(
                Consumer<WorkerTaskNode> action,
                @Nullable ActorSchedulerThreadPool.WorkerTaskNode insideTask,
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

    private final class WorkerTaskNode {
        private final SchedulableTick internal;
        private final MultiThreadedQueue<InternalMessage> subMessageNodes = new MultiThreadedQueue<>();

        private long tickTimeDeadlineBuffer = ActorSchedulerThreadPool.this.schedulerConfig.maxTickTimeWaitBuffer;
        private long mainThreadTaskPeriod = ActorSchedulerThreadPool.this.schedulerConfig.maxTaskDeadlineBuffer;

        private SchedulerWorkerCarrier ownerWorker;

        private boolean wannaReinsert = true;
        private boolean executed = false;

        private boolean mainThreadTaskInterrupted = false;
        private boolean pushTickWithinMinTickDeadlineBuffer = false;

        private final AtomicLong lastTickExecutionTimeSum = new AtomicLong();
        private final AtomicLong executedTickCount = new AtomicLong();

        private final AtomicLong lastDeadlineApproachSum = new AtomicLong();
        private final AtomicInteger passedTimes = new AtomicInteger();

        private static final VarHandle OWNER_HANDLE = ConcurrentUtil.getVarHandle(WorkerTaskNode.class, "ownerWorker", SchedulerWorkerCarrier.class);

        private WorkerTaskNode(SchedulableTick internal) {
            this.internal = internal;
        }

        public boolean tryPreDispatch(SchedulerWorkerCarrier carrier) {
            return this.trySetWorker(carrier);
        }

        public void setWorker(SchedulerWorkerCarrier carrier) {
            OWNER_HANDLE.setVolatile(this, carrier);
        }

        public boolean trySetWorker(SchedulerWorkerCarrier ownerWorker) {
            return OWNER_HANDLE.compareAndSet(this, null, ownerWorker);
        }

        private void cleanOwnerWorker() {
            OWNER_HANDLE.setVolatile(this, null);
        }

        private SchedulerWorkerCarrier getOwnerWorker() {
            return (SchedulerWorkerCarrier) OWNER_HANDLE.getVolatile(this);
        }

        public void notifyReceiver() {
            final SchedulerWorkerCarrier owner = (SchedulerWorkerCarrier) OWNER_HANDLE.getVolatile(this);

            if (owner == null) {
                return;
            }

            LockSupport.unpark(owner.runner);
        }

        private void resetContextFlags() {
            this.mainThreadTaskInterrupted = false;
            this.pushTickWithinMinTickDeadlineBuffer = false;
        }

        public boolean sendMessage(InternalMessage internalMessage) {
            return this.subMessageNodes.offer(internalMessage);
        }

        public void doMessageProcess() {
            boolean canceled = false;

            try {
                final AtomicInteger executedCount = new AtomicInteger(0);

                final long tickDeadline = this.internal.getScheduledStart();
                final long taskDeadline = System.nanoTime() + this.mainThreadTaskPeriod;
                long remaining = System.nanoTime() - tickDeadline;

                this.lastDeadlineApproachSum.getAndAdd(tickDeadline - System.nanoTime());
                this.passedTimes.getAndIncrement();

                // run tasks once if buffer(tick + task) is enough for extra task execution, or we will do task run during the wait stage
                if (this.internal.hasTasks() && (remaining + this.tickTimeDeadlineBuffer) < 0) {
                    canceled = !this.internal.runTasks(() -> {
                        this.processSubMessageNode();

                        final long remainingCurr = System.nanoTime() - taskDeadline;

                        executedCount.incrementAndGet();
                        return remainingCurr > 0 && !this.mainThreadTaskInterrupted;
                    });
                }

                this.executed = executedCount.get() > 0;

                if (canceled) {
                    return;
                }

                // we have enough time for this tick, so reinsert back for load balance
                if ((remaining + this.tickTimeDeadlineBuffer) < 0) {
                    return;
                }

                // we pushed tick for more task runs
                if (this.pushTickWithinMinTickDeadlineBuffer && (remaining + ActorSchedulerThreadPool.this.schedulerConfig.minTickTimeWaitBuffer) < 0) {
                    return;
                }

                int taskExecutionFailure = 0;
                for (;;) {
                    this.processSubMessageNode();

                    remaining = System.nanoTime() - tickDeadline;

                    // might someone notified for a task execution within its min time buffer
                    if (this.pushTickWithinMinTickDeadlineBuffer && (remaining + ActorSchedulerThreadPool.this.schedulerConfig.minTickTimeWaitBuffer) < 0) {
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

                            taskExecutionFailure = 0;
                            continue;
                        }

                        taskExecutionFailure++;

                        Thread.yield();
                        LockSupport.parkNanos("AWAIT DEADLINE", Math.min(10, taskExecutionFailure) * 1000L);
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
            final InternalMessage internalMessage = this.subMessageNodes.poll();

            // might have an interrupt message incoming
            if (internalMessage != null) {
                internalMessage.process();
            }
        }

        public void finalizeSubMsgBelongToSelf(boolean canceled) {
            InternalMessage internalMessage;
            while ((internalMessage = canceled ? this.subMessageNodes.pollOrBlockAdds() : this.subMessageNodes.poll()) != null) {
                try {
                    internalMessage.doFinalized();
                }catch (Exception ex) {
                    final SchedulerWorkerCarrier owner = this.getOwnerWorker();

                    ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(owner != null ? owner.runner : null, ex);
                }
            }
        }

        public void onCancelled() {
            this.finalizeSubMsgBelongToSelf(true);
        }
    }

    // use this to implement the "pollOrBlockAdd function like MultiThreadedQueue"
    private static final class WorkerQueueConditioner {
        private int referenceCount = 0;
        private boolean addBlocked = false;

        private static final VarHandle REFERENCE_COUNT_HANDLE = ConcurrentUtil.getVarHandle(WorkerQueueConditioner.class, "referenceCount", int.class);
        private static final VarHandle BLOCK_ADD_HANDLE = ConcurrentUtil.getVarHandle(WorkerQueueConditioner.class, "addBlocked", boolean.class);
        
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

    private final class SchedulerWorkerCarrier implements Runnable {
        private static final Comparator<WorkerTaskNode> TICK_COMPARATOR_BY_TIME = (t1, t2) -> {
            int timeCompare = TimeUtil.compareTimes(t1.internal.scheduledStart, t2.internal.scheduledStart);
            return timeCompare != 0 ? timeCompare : Long.compare(t1.internal.id, t2.internal.id);
        };

        public static final int STATUS_IDLE = 0;
        public static final int STATUS_SHUTDOWN = 1;
        public static final int STATUS_BUSY = 2;
        public static final int STATUS_RUNNING = 3;
        public static final int STATUS_STEALING_TASKS = 4;
        public static final int STATUS_TASK_GOT_FROM_STEAL = 5;

        private final Thread runner;
        private final ConcurrentSkipListSet<WorkerTaskNode> inComingTaskMessages = new ConcurrentSkipListSet<>(TICK_COMPARATOR_BY_TIME);
        private final WorkerQueueConditioner queueConditioner = new WorkerQueueConditioner();

        private final AtomicBoolean killSignal = new AtomicBoolean(false);
        private final AtomicInteger status = new AtomicInteger(0);

        private SchedulerWorkerCarrier(@NotNull ThreadFactory factory) {
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
                final boolean killed = this.killSignal.get();
                WorkerTaskNode incomingMessage = this.takeMessage(killed);

                // no more task stay in curr thread and we were killed
                if (killed && incomingMessage == null) {
                    break;
                }

                if (incomingMessage != null) {
                    this.status.set(STATUS_BUSY);

                    // pull out curr thread from idle threads if possible
                    ActorSchedulerThreadPool.this.idleWorkers.remove(this);

                    Pair<Boolean, Boolean> result = this.processMessage(incomingMessage);

                    final boolean wannaReinsert = result.left();
                    final boolean executed = result.right();

                    if (wannaReinsert) {
                        ActorSchedulerThreadPool.this.dispatchMessageNodeAuto(incomingMessage, true);
                    }else {
                        // task retired, remove it from the task list

                        incomingMessage.onCancelled();

                        ActorSchedulerThreadPool.this.removeMessageNode(incomingMessage);
                    }

                    if (executed) {
                        executeFailureCount = 0;
                        continue;
                    }

                    executeFailureCount++;
                } else {
                    // steal some task from other busy threads
                    // here we won't increase the executeFailed cnt when steal failed as we are not running tasks of ourselves
                    this.status.set(STATUS_STEALING_TASKS);
                    SchedulerWorkerCarrier other = this.randomSelect();
                    final int maxStealAttempts = ActorSchedulerThreadPool.this.workers.size();

                    for (int i = 0; i < maxStealAttempts; i++) {
                        if (other != null && other != this) {
                            // skip those who got tasks from stealing
                            if (other.status.get() ==  STATUS_STEALING_TASKS || other.status.get() == STATUS_TASK_GOT_FROM_STEAL) {
                                continue;
                            }

                            break;
                        }

                        other = this.randomSelect();
                    }

                    if (other != null && other != this) {
                        incomingMessage = other.steal();
                    }

                    if (incomingMessage != null) {
                        // it is in our queue now
                        incomingMessage.setWorker(this);

                        this.inComingTaskMessages.add(incomingMessage);

                        // we need to prevent the task got stolen twice
                        this.status.set(STATUS_TASK_GOT_FROM_STEAL);

                        // pull out curr thread from idle threads if possible
                        ActorSchedulerThreadPool.this.idleWorkers.remove(this);
                        continue;
                    }
                }

                // push to idle threads
                ActorSchedulerThreadPool.this.idleWorkers.offer(this);

                this.status.set(STATUS_IDLE);

                executeFailureCount++;

                // sleep 1 - 100us based on load
                long parkNanos = Math.min(Math.max(executeFailureCount * 1000L, 1000L), 100000L);
                
                LockSupport.parkNanos("IDLE", parkNanos);
            }

            this.status.set(STATUS_SHUTDOWN);
        }

        @Contract("_ -> new")
        private @NotNull Pair<Boolean, Boolean> processMessage(@NotNull ActorSchedulerThreadPool.WorkerTaskNode node) {

            try {
                node.doMessageProcess();
            }catch (Exception ex) {
                ActorSchedulerThreadPool.this.exceptionHandler.uncaughtException(this.runner, ex);
            }

            return Pair.of(node.wannaReinsert, node.executed);
        }

        private WorkerTaskNode takeMessage(boolean blockAdd) {
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

        private WorkerTaskNode steal() {
            return this.inComingTaskMessages.pollFirst();
        }

        private boolean message(WorkerTaskNode messageNode) {
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

        private SchedulerWorkerCarrier randomSelect() {
            final SchedulerWorkerCarrier[] allSchedulers = ActorSchedulerThreadPool.this.workers.toArray(new SchedulerWorkerCarrier[0]);
            final ThreadLocalRandom random = ThreadLocalRandom.current();

            return allSchedulers[random.nextInt(allSchedulers.length)];
        }
    }
}
