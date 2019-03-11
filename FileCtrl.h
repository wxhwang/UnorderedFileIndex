#ifndef FILE_CTRL_H_
#define FILE_CTRL_H_

#include <sys/types.h>
#include <string>
#include <pthread.h>
using namespace std;

class FileCtrl
{
private:
    string m_filePath; // 文件路径
    uint8_t *m_startAddr; // 文件映射到内存后返回的地址
    // 此处要改称64位，支持大文件
    size_t m_fileLen; // 映射到内存的文件大小
    pthread_mutex_t m_mutex; // 多线程并发构建索引并发控制
    uint64_t m_TraverseOffset; // 当前遍历位置
    
    const uint32_t SEPERATOR_LEN; // ","分隔符空间占用大小
    const uint32_t VALUE_SIZE_LEN; // value长度空间占用大小
    
    size_t GetFileSize(); // 获取文件大小
    
public:
    FileCtrl(string filePath) : SEPERATOR_LEN(1), VALUE_SIZE_LEN(sizeof(uint64_t))
    {
        this->m_filePath = filePath;
        this->m_fileLen = 0;
        this->m_startAddr = NULL;
        this->m_TraverseOffset = 0;
        pthread_mutex_init(&this->m_mutex, NULL);
    }
    
    ~FileCtrl();
    int32_t DoMmap(); // 将文件映射到内存
    void ReadValue(uint64_t valueOffset, uint64_t valueLen, uint8_t **outValue); // 从value偏移位置读取value
    void ReadValueAt(uint64_t valueLenOffset, uint8_t **outValue, uint64_t *outValueLen); // 从valueLen偏移位置读取value
    uint64_t ReadKVAt(uint64_t keyLenOffset, uint8_t **outKey, uint64_t *outKeyLen, uint8_t **outValue, uint64_t *outValueLen); // 从key偏移位置读取key&value，返回下一条记录开始位置
    bool IsOffsetValid(uint64_t offset); // 检查offset是否有效
    
    // 并发构建索引
    void BeginTraverse();
    int32_t DoTraverse(uint8_t **outKey, uint64_t *outKeyLen, uint8_t **outValue, uint64_t *outValueLen);
};

#endif
