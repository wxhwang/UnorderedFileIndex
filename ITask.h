#ifndef ITASK_H_
#define ITASK_H_

#include <string>
#include <sys/types.h>
using namespace std;


// 任务执行完回调函数
typedef void (*TASK_CALLBACK_FUNC)(void *args);

class ITask
{
protected:
    string m_taskName; // 任务名称
    void* m_taskData; // 执行任务的具体数据
    void *m_cbArgs; // callback函数参数
    TASK_CALLBACK_FUNC callback; // callback函数
    
public:
    ITask(string taskName, void *taskData)
    {
        this->m_taskName = taskName;
        this->m_taskData = taskData;
        this->m_cbArgs = NULL;
        this->callback = NULL;
    }
    
    void SetCallbackFunc(TASK_CALLBACK_FUNC cb, void *args)
    {
        this->callback = cb;
        this->m_cbArgs = args;
    }
    
    virtual ~ITask()
    {
    }
    virtual int32_t Run() = 0;
};

#endif
