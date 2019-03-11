#include <string>
#include <iostream>
#include "ThreadPool.h"

pthread_mutex_t ThreadPool::s_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t ThreadPool::s_cond = PTHREAD_COND_INITIALIZER;

void *ThreadPool::ThreadEntryFunc(void *threadData)
{
    pthread_t tid = pthread_self();
    while(1)
    {
        pthread_mutex_lock(&s_mutex);
        cout << "tid:" << tid << " run" << endl;
        // 取任务执行
        queue<ITask*> *taskQ = (queue<ITask*> *)threadData;
        while(taskQ->empty())
        {
            pthread_cond_wait(&s_cond, &s_mutex);
        }
        
        ITask *task = taskQ->front();
        taskQ->pop();
        pthread_mutex_unlock(&s_mutex);
        task->Run();
        
    }
    return (void*)0;
}

void ThreadPool::Create()
{
    for(int i = 0; i < m_threadNum; i++)
    {
        pthread_t tid = 0;
        pthread_create(&tid, NULL, ThreadEntryFunc, &this->m_taskQ);
        this->m_threadQ.push(tid);
    }
}

ThreadPool::ThreadPool(int threadNum)
{
    this->m_threadNum = threadNum;
    Create();
}


// 添加一条任务到任务队列中
int32_t ThreadPool::AddTask(ITask *task)
{
    pthread_mutex_lock(&s_mutex);
    this->m_taskQ.push(task);
    pthread_cond_signal(&s_cond);
    pthread_mutex_unlock(&s_mutex);
    
    return 0;
}

ThreadPool::~ThreadPool()
{
    while(!this->m_threadQ.empty())
    {
        pthread_t thread = this->m_threadQ.front();
        this->m_threadQ.pop();
        pthread_cancel(thread);
        pthread_join(thread, NULL);
    }
    
    while(!this->m_taskQ.empty())
    {
        ITask *task = this->m_taskQ.front();
        this->m_taskQ.pop();
        delete task;
    }
}
