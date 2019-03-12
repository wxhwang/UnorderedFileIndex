#ifndef PRE_PROCESS_H_
#define PRE_PROCESS_H_

#include <atomic>
#include "ITask.h"
#include "FileCtrl.h"
#include "MappingTable.h"
#include "ThreadPool.h"

// 预处理模块创建8个任务并行构建索引
class PreProcess
{
private:
    FileCtrl *m_fileCtrl;
    MappingTable *m_mappingTable;
    ThreadPool *m_threadPool;
    int32_t m_tasksNum; // 需要创建的任务数量
    atomic_int32_t m_runningTasksNum; // 正在运行的任务数量
    
public:
    friend class BuildIndexTask;
    friend void BuildIndexTaskCallBack(void *args);
    PreProcess(int32_t tasksNum)
    {
        this->m_tasksNum = tasksNum;
        this->m_runningTasksNum = 0;
    }
    void SetFileCtrl(FileCtrl *fileCtrl)
    {
        this->m_fileCtrl = fileCtrl;
    }
    void SetMappingTable(MappingTable *mappingTable)
    {
        this->m_mappingTable = mappingTable;
    }
    void SetThreadPool(ThreadPool *threadPool)
    {
        this->m_threadPool = threadPool;
    }
    
    void CreateTasks(); // 创建多个构建索引任务
};


// 构建索引任务
class BuildIndexTask:public ITask
{
private:
    PreProcess *m_processIns;
    
public:
    BuildIndexTask(PreProcess *processIns):ITask("BuildIndexTask")
    {
        this->m_processIns = processIns;
    }
    int32_t Run(); // 执行体
};

#endif
