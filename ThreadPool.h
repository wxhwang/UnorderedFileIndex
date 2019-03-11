#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <sys/types.h>
#include <queue>
#include <pthread.h>
#include "ITask.h"

using namespace std;

class ThreadPool
{
private:
    queue<ITask *> m_taskQ; // 任务队列
    queue<pthread_t> m_threadQ; // 线程队列
    int32_t m_threadNum; // 线程池中的线程数目
    static pthread_mutex_t s_mutex; // 同步锁
    static pthread_cond_t s_cond; // 条件变量
    
protected:
    static void *ThreadEntryFunc(void *threadData); // 线程的入口函数
    void Create(); // 创建所有线程
    
public:
    ThreadPool(int threadNum);
    ~ThreadPool();
    int32_t AddTask(ITask *task); // 添加任务到线程池中
};

#endif
