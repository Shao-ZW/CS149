#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    std::thread* threads = new std::thread[num_threads];

    for (int i = 0; i < num_threads; i++) {
        threads[i] = std::thread([this, runnable, i, num_total_tasks]() {
            for(int tid = i; tid < num_total_tasks; tid += num_threads) {
                runnable->runTask(tid, num_total_tasks);
            }
        });
    }

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    delete[] threads;
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    stop = false;
    current_task_id = num_total_tasks = 0;

    for(int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this] {
            while(1) {
                if(stop)    return;
                int task_id = -1;
                mutex_.lock();
                if(current_task_id < num_total_tasks) {
                    task_id = current_task_id++;
                }
                mutex_.unlock();
                if(task_id != -1) {
                    runnable->runTask(task_id, num_total_tasks);
                    num_done_tasks.fetch_add(1);
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop = true;
    for(std::thread& thread: threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    mutex_.lock();
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    current_task_id = 0;
    num_done_tasks = 0;
    mutex_.unlock();

    while (num_done_tasks < num_total_tasks)     
        ;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    stop = false;
    current_task_id = num_total_tasks = 0;

    for(int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this] {
            while(1) {
                if(stop)    return;
                int task_id = -1;
                mutex_.lock();
                if(current_task_id < num_total_tasks) {
                    task_id = current_task_id++;
                }
                mutex_.unlock();
                if(task_id != -1) {
                    runnable->runTask(task_id, num_total_tasks);
                    num_done_tasks.fetch_add(1);
                } else {
                    std::unique_lock<std::mutex> lk(mutex_);
                    condition_variable_.wait(lk);
                    lk.unlock();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    stop = true;
    condition_variable_.notify_all();
    for(std::thread& thread: threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    mutex_.lock();
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    current_task_id = 0;
    num_done_tasks = 0;
    mutex_.unlock();

    condition_variable_.notify_all();
    while (num_done_tasks < num_total_tasks)     
        ;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
