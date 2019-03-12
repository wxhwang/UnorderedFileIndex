#ifndef MAPPING_TABLE_H_
#define MAPPING_TABLE_H_

#include <sys/types.h>
#include <sys/mman.h>
#include <atomic>
#include <string>
#include "FileCtrl.h"

using namespace std;

// 哈希表中存储的key最大长度，超过长度只存储hash值
#define MAPPING_TABLE_MAX_KEY_LEN                 (6)
// 哈希表中存储的key hash值的长度
#define MAPPING_TABLE_HASH_LEN                    (6)
// 哈希表中存储的value最大长度，超过长度只存储地址
#define MAPPING_TABLE_MAX_VALUE_LEN               (8)

typedef enum KEY_VALUE_TYPE_E
{
    KEY_AND_VALUE               = 0, // 哈希表中的值是key和value
    KEY_AND_VALUE_ADDR          = 1, // 哈希表中的值是key和value地址
    HASH_AND_KEY_VALUE_ADDR     = 2, // 哈希表中的值是hash值和key+value的地址
}KEY_VALUE_TYPE_E;

// 8B
typedef struct DiskAddr
{
    uint64_t offset:40;
    uint64_t size:24;
}DiskAddr;

// 24B
class Entry
{
public:
    /* type决定了hash表中存储的KV如何解析
     1. 当key长度小于等于6B，并且value长度小于等于8B时，设置type = KEY_AND_VALUE，KV直接存储在哈希表中。
        此时不需要去数据文件中读，优化小key小value下索引表读性能
     2. 当key长度小于等于6B，并且value长度大于8B时，设置type = KEY_AND_VALUE_ADDR，此时哈希表中存储的是key和value的地址。
        此时需要去数据文件中读value，优化大value下索引空间占用
     3. 当key长度大于6B时，设置type=HASH_AND_KEY_VALUE_ADDR，此时哈希表中存储的是key的hash值(6B)，以及KV对在file上的地址。
        此时在比较时需要从file上读取数据，但可以认为hash值冲突概率极小，需要去数据文件中读取KV值
     */
    uint16_t type:2; // KEY_VALUE_TYPE_E类型
    uint16_t keyLen:3; // 哈希表中存储的key或者hash值长度
    uint16_t valueLen:4; // 哈希表中存储的value长度
    union
    {
        struct
        {
            uint8_t key[MAPPING_TABLE_MAX_KEY_LEN]; // 存储key，key的实际长度由keyLen决定
            union
            {
                uint8_t value[MAPPING_TABLE_MAX_VALUE_LEN]; // 存储value，value的实际长度由valueLen决定
                DiskAddr valueAddr; // 存储value地址
            };
        };
        
        struct
        {
            uint8_t hash[MAPPING_TABLE_HASH_LEN]; // 存储key的hash值，hash值的实际长度由keyLen决定
            DiskAddr addr; // 存储KV地址
        };
    };
    
    uint64_t next; // 使用编号组织冲突链
    
    void Init(uint8_t *key, uint64_t keyOffset, uint64_t keyLen, uint8_t *value, uint64_t valueOffset, uint64_t valueLen, uint64_t kvOffset, uint64_t kvLen);
};

// 8B
typedef struct Bucket
{
    uint64_t next_entry_idx; // 组织冲突链，链表头结点
}Bucket;

class MappingTable
{
private:
    // 将文件映射内存，前面用来组织hash桶空间，后面采用Log方式分配entry
    string m_memMappingFilePath;
    // 此次要改成64位，支持大文件
    size_t m_fileLen; // 映射内存大小
    uint8_t *m_startAddr; // 文件映射到内存后返回的地址
    Bucket *m_buckets; // 哈希桶，需要使用mlock函数锁定内存不允许交换出去，等到索引表构建完成后，再持久化到文件中s
    uint64_t m_bucketsNum; // 2的幂次方
    
    Entry *m_entries;
    uint64_t m_totalEntriesNum;
    // 此处要使用原子变量
    atomic_uint64_t m_allocIdx; // 当前分配的entry位置
    
    FileCtrl *m_fileCtrl; // 用来根据地址读文件
    
    uint64_t AllocEntry();
    uint64_t CalculHash(const void* buffer, size_t length);
    Bucket *GetBucket(uint64_t hash);
public:
    MappingTable(string mappingFilePath, uint64_t bucketsNum, uint64_t entriesNum)
    {
        this->m_memMappingFilePath = mappingFilePath;
        this->m_bucketsNum = bucketsNum;
        this->m_totalEntriesNum = entriesNum;
        this->m_buckets = NULL;
        this->m_entries = NULL;
        this->m_fileCtrl = NULL;
        this->m_startAddr = NULL;
        this->m_allocIdx = 0;
        this->m_fileLen = 0;
    }
    ~MappingTable();
    int32_t DoMmap();
    void SetUpFileCtrl(FileCtrl *fileCtrl);
    
    // 插入一条记录，支持并发
    int32_t Put(uint8_t *key, uint64_t keyOffset, uint64_t keyLen, uint8_t *value, uint64_t valueOffset, uint64_t valueLen, uint64_t kvOffset, uint64_t kvLen);
    int32_t Get(uint8_t *key, uint64_t keyLen, uint8_t **outValue, uint64_t *outValueLen);
    
    // 索引构建完成，持久化哈希桶内存
    int32_t IndexBuildComplete();
};


#endif
