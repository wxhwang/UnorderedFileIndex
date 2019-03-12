#define _GNU_SOURCE
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include "FileCtrl.h"

size_t FileCtrl::GetFileSize()
{
    struct stat buf;
    if(stat(this->m_filePath.c_str(), &buf) < 0)
    {
        cout << "stat " << this->m_filePath << " failed." << endl;
        return -1;
    }
    
    return buf.st_size;
}

// 映射文件到内存
int32_t FileCtrl::DoMmap()
{
    int fd;
    size_t fileLen;
    void *startAddr;
    
    // 1. 打开文件
    fd = open(this->m_filePath.c_str(), O_RDONLY);
    if(-1 == fd)
    {
        cout << "open " << this->m_filePath << " failed." << endl;
        return -1;
    }
    
    // 2. 获取文件长度
    fileLen = GetFileSize();
    if(-1 == fileLen)
    {
        cout << "do get " << this->m_filePath << " size failed." << endl;
        return -1;
    }
    
    // 3. mmap文件
    startAddr = mmap(0, fileLen, PROT_READ, MAP_SHARED, fd, 0);
    if(startAddr == (void *)-1)
    {
        cout << "do mmap " << this->m_filePath << " failed." << endl;
        return -1;
    }
    
    // 4. 关闭文件
    close(fd);
    
    cout << "mmap " << this->m_filePath << " size " << fileLen << " succeed." << endl;
    
    this->m_fileLen = fileLen;
    this->m_startAddr = (uint8_t *)startAddr;
    return 0;
}

void FileCtrl::ReadValue(uint64_t valueOffset, uint64_t valueLen, uint8_t **outValue)
{
    *outValue = this->m_startAddr + valueOffset;
}

void FileCtrl::ReadValueAt(uint64_t valueLenOffset, uint8_t **outValue, uint64_t *outValueLen)
{
    uint8_t *data = this->m_startAddr + valueLenOffset;
    // 获取value长度
    *outValueLen = *(uint64_t *)data;
    data += FileCtrl::VALUE_SIZE_LEN;
    data += FileCtrl::SEPERATOR_LEN;
    *outValue = data;
}

uint64_t FileCtrl::ReadKVAt(uint64_t keyLenOffset, uint8_t **outKey, uint64_t *outKeyLen, uint8_t **outValue, uint64_t *outValueLen)
{
    uint64_t offset = keyLenOffset;
    // 获取key长度
    uint8_t *data = this->m_startAddr + offset;
    *outKeyLen = *(uint64_t *)data;
    offset += FileCtrl::VALUE_SIZE_LEN;
    offset += FileCtrl::SEPERATOR_LEN;
    
    // 获取key
    data = this->m_startAddr + offset;
    *outKey = data;
    offset += *outKeyLen;
    offset += FileCtrl::SEPERATOR_LEN;
    
    // 获取value长度
    data = this->m_startAddr + offset;
    *outValueLen = *(uint64_t *)data;
    offset += FileCtrl::VALUE_SIZE_LEN;
    offset += FileCtrl::SEPERATOR_LEN;
    
    // 获取value
    data = this->m_startAddr + offset;
    *outValue = data;
    offset += *outValueLen;
    offset += FileCtrl::SEPERATOR_LEN;
    
    //返回下一条记录开始位置
    return offset;
}

bool FileCtrl::IsOffsetValid(uint64_t offset)
{
    return offset < this->m_fileLen;
}

void FileCtrl::BeginTraverse()
{
    this->m_TraverseOffset = 0;
}

int32_t FileCtrl::DoTraverse(uint8_t **outKey, uint64_t *outKeyOffset, uint64_t *outKeyLen, uint8_t **outValue, uint64_t *outValueOffset, uint64_t *outValueLen, uint64_t *outKvOffset, uint64_t *outKvLen)
{
    pthread_mutex_lock(&m_mutex);
    if(!IsOffsetValid(this->m_TraverseOffset))
    {
        // 遍历结束
        pthread_mutex_unlock(&m_mutex);
        return -1;
    }
    
    this->m_TraverseOffset = ReadKVAt(this->m_TraverseOffset, outKey, outKeyLen, outValue, outValueLen);
    pthread_mutex_unlock(&m_mutex);
    
    return 0;
}

FileCtrl::~FileCtrl()
{
    if(NULL != this->m_startAddr)
    {
        munmap(this->m_startAddr, this->m_fileLen);
    }
    pthread_mutex_destroy(&this->m_mutex);
}
