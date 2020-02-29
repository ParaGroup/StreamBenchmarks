/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.affinity;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LockInventory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockInventory.class);
    /**
     * The locks belonging to physical cores. Since a physical core can host multiple logical cores
     * the relationship is one to many.
     */
    private final NavigableMap<Integer, AffinityLock[]> physicalCoreLocks = new TreeMap<>();
    private CpuLayout cpuLayout;
    /**
     * The lock belonging to each logical core. 1-to-1 relationship
     */
    private AffinityLock[] logicalCoreLocks;

    public LockInventory(CpuLayout cpuLayout) {
        set(cpuLayout);
    }

    public static String dumpLocks(@NotNull AffinityLock[] locks) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < locks.length; i++) {
            AffinityLock al = locks[i];
            sb.append(i).append(": ");
            sb.append(al.toString());
            sb.append('\n');
        }
        return sb.toString();
    }

    private static boolean anyStrategyMatches(final int cpuOne, final int cpuTwo, final AffinityStrategy[] strategies) {
        for (AffinityStrategy strategy : strategies) {
            if (strategy.matches(cpuOne, cpuTwo)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAnyCpu(final int cpuId) {
        return cpuId == AffinityLock.ANY_CPU;
    }

    private static void updateLockForCurrentThread(final boolean bind, final AffinityLock al, final boolean b) {
        al.assignCurrentThread(bind, b);
        LockCheck.updateCpu(al.cpuId());
    }

    public final synchronized CpuLayout getCpuLayout() {
        return cpuLayout;
    }

    public final synchronized void set(CpuLayout cpuLayout) {
        if (cpuLayout.equals(this.cpuLayout)) {
            return;
        }
        reset(cpuLayout);
//		LOGGER.info("Configure cpu layout...");
        for (int i = 0; i < cpuLayout.cpus(); i++) {
            final boolean base = AffinityLock.BASE_AFFINITY.get(i);
            final boolean reservable = AffinityLock.RESERVED_AFFINITY.get(i);
//			LOGGER.info("logical cpu={}  base={} reservable= {}", i, base, reservable);

            AffinityLock lock = logicalCoreLocks[i] = newLock(i, false, true);//the original assumption is that the system should avoid competing with the ``current thread's" affinity.

            int layoutId = lock.cpuId();
            int physicalCore = toPhysicalCore(layoutId);
//			LOGGER.info("physicalCore:" + physicalCore + " logical Core:" + layoutId);


            AffinityLock[] locks = physicalCoreLocks.get(physicalCore);
            if (locks == null) {
                physicalCoreLocks.put(physicalCore, locks = new AffinityLock[cpuLayout.threadsPerCore()]);
            }
            locks[cpuLayout.threadId(layoutId)] = lock;
        }
//		System.exit(-1);
    }

    public final synchronized AffinityLock acquireLock(boolean bind, int cpuId, AffinityStrategy... strategies) {
        final boolean specificCpuRequested = !isAnyCpu(cpuId);
        if (specificCpuRequested && cpuId != 0) {
            final AffinityLock required = logicalCoreLocks[cpuId];
//			LOGGER.info("logical cpu={} reservable= {}", cpuId, required.canReserve());//check
            if (required.canReserve() && anyStrategyMatches(cpuId, cpuId, strategies)) {
                updateLockForCurrentThread(bind, required, false);
                return required;
            }

            if (!required.canReserve()) {
                LOGGER.warn("Unable to acquire lock on CPU {} for thread {} because it is not reservable",
                        cpuId, Thread.currentThread());
            } else if (!anyStrategyMatches(cpuId, cpuId, strategies)) {
                LOGGER.warn("Unable to acquire lock on CPU {} for thread {} because strategy not matching.",
                        cpuId, Thread.currentThread());
            }
        }

        LOGGER.warn("Unable to acquire lock on CPU {} for thread {}, trying to find another CPU",
                cpuId, Thread.currentThread());

        for (AffinityStrategy strategy : strategies) {
            // consider all processors except cpu 0 which is usually used by the OS.
            // if you have only one core, this library is not appropriate in any case.
            for (int i = 0; i < logicalCoreLocks.length; i++) { //for (int i = logicalCoreLocks.length - 1; i > 0; i--)
                AffinityLock al = logicalCoreLocks[i];
                if (al.canReserve() && (isAnyCpu(cpuId) || strategy.matches(cpuId, al.cpuId()))) {
                    updateLockForCurrentThread(bind, al, false);
                    LOGGER.info("instead.. bind to cpu:" + i);
                    return al;
                }
            }
        }

        LOGGER.warn("No reservable CPU for {}", Thread.currentThread());

        return newLock(AffinityLock.ANY_CPU, false, false);
    }

    public final synchronized AffinityLock acquireCore(boolean bind, int cpuId, AffinityStrategy... strategies) {

        for (AffinityStrategy strategy : strategies) {
//			for (AffinityLock[] als : physicalCoreLocks.descendingMap().values()) {
            int CORE = toPhysicalCore(cpuId);
            LOGGER.info("Binding to physical CORE:" + CORE);
            AffinityLock[] als = physicalCoreLocks.get(CORE);
//			LOOP:
//			while (als == null) {
//				for (Map.Entry<Integer, AffinityLock[]> tmp : physicalCoreLocks.entrySet()) {
//					for (AffinityLock al : tmp.getValue()) {
//						LOGGER.info("physical core:" + tmp.getKey() + ", logical core: " + al.cpuId() + ":" + al.toString());
//					}
//				}
//				als = physicalCoreLocks.get(cpuId + 1);
//
//			}

            for (AffinityLock al : als) {
                LOGGER.info("That is, binding to:" + al.cpuId());

                if (!al.canReserve() || !strategy.matches(cpuId, al.cpuId())) {
//					continue LOOP;
                    LOGGER.info("Reserve whole core failed.");
                    System.exit(-1);
                }
            }

            final AffinityLock al = als[0];
            updateLockForCurrentThread(bind, al, true);
            return al;
//			}
        }

        LOGGER.warn("No reservable Core for {}", Thread.currentThread());

        return acquireLock(bind, cpuId, strategies);
    }

    public final synchronized void bindWholeCore(int logicalCoreID) {
        if (logicalCoreID < 0) {
            LOGGER.warn("Can't bind core since it was not possible to reserve it!");
            return;
        }

        int core = toPhysicalCore(logicalCoreID);
        for (AffinityLock al : physicalCoreLocks.get(core)) {
            if (al.isBound() && al.assignedThread != null && al.assignedThread.isAlive()) {
                LOGGER.warn("cpu {} already bound to {}", al.cpuId(), al.assignedThread);

            } else {
                al.bound = true;
                al.assignedThread = Thread.currentThread();
            }
        }

        if (LOGGER.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder().append("Assigning core ").append(core);
            String sep = ": cpus ";
            for (AffinityLock al : physicalCoreLocks.get(core)) {
                sb.append(sep).append(al.cpuId());
                sep = ", ";
            }
            sb.append(" to ").append(Thread.currentThread());
            LOGGER.info(sb.toString());
        }
    }

    public final synchronized void release() {
        Thread t = Thread.currentThread();
        for (AffinityLock al : logicalCoreLocks) {
            Thread at = al.assignedThread;
            if (at == t) {
                releaseAffinityLock(t, al, "Releasing cpu {} from {}");
            } else if (at != null && !at.isAlive()) {
                releaseAffinityLock(t, al, "Releasing cpu {} from {} as it is not alive.");
            }
        }
        Affinity.resetToBaseAffinity();
    }

    public final synchronized String dumpLocks() {
        return dumpLocks(logicalCoreLocks);
    }

    protected AffinityLock newLock(int cpuId, boolean base, boolean reservable) {
        return new AffinityLock(cpuId, base, reservable, this);
    }

    private void reset(CpuLayout cpuLayout) {
        this.cpuLayout = cpuLayout;
        this.logicalCoreLocks = new AffinityLock[cpuLayout.cpus()];
        this.physicalCoreLocks.clear();
    }

    private int toPhysicalCore(int layoutId) {
        return cpuLayout.socketId(layoutId) * cpuLayout.coresPerSocket() + cpuLayout.coreId(layoutId);
    }

    private void releaseAffinityLock(final Thread t, final AffinityLock al, final String format) {
        LOGGER.debug(format, al.cpuId(), t);
        al.assignedThread = null;
        al.bound = false;
        al.boundHere = null;

        final String lockFilePath = LockCheck.toFile(al.cpuId()).getAbsolutePath();
        try {
            Files.delete(Paths.get(lockFilePath));
        } catch (IOException e) {
            LOGGER.debug("Failed to delete lock file at " + lockFilePath);
        }
    }
}