//
//  PreProcess.cpp
//  PingCapJob
//
//  Created by wangxiaohui on 2019/3/12.
//  Copyright © 2019 wangxiaohui. All rights reserved.
//

#include "PreProcess.h"

void BuildIndexTaskCallBack(void *args)
{
    PreProcess *processIns = (PreProcess  *)args;
    if(1 == atomic_fetch_sub(&processIns->m_runningTasksNum, 1))
    {
        // 所有任务都返回，通知索引表持久化哈希桶内存
        processIns->m_mappingTable->IndexBuildComplete();
    }
}

void PreProcess::CreateTasks()
{
    m_fileCtrl->BeginTraverse();
    atomic_store(&m_runningTasksNum, m_tasksNum);
    for(int32_t i = 0; i < m_tasksNum; i++)
    {
        // 创建任务并加入到线程池中
        ITask *task = new BuildIndexTask(this);
        task->SetCallbackFunc(BuildIndexTaskCallBack, (void *)this);
        m_threadPool->AddTask(task);
    }
    
}

int32_t BuildIndexTask::Run()
{
    // 遍历获取key-value记录
    uint8_t *key, *value;
    uint64_t keyOffset, keyLen, valueOffest, valueLen, kvOffset, kvLen;
    while(-1 != m_processIns->m_fileCtrl->DoTraverse(&key, &keyOffset, &keyLen, &value, &valueOffest, &valueLen, &kvOffset, &kvLen))
    {
        m_processIns->m_mappingTable->Put(key, keyOffset, keyLen, value, valueOffest, valueLen, kvOffset, kvLen);
    }
    
    // 任务执行完成，调用callback返回通知调用方
    callback(m_cbArgs);
    
    return 0;
}

