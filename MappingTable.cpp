#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <iostream>
#include "xxhash.h"
#include "MappingTable.h"

// 标识链表结束
#define INVALID_ENTRY_NO                    (-1)


void Entry::Init(uint8_t *key, uint64_t keyOffset, uint64_t keyLen, uint8_t *value, uint64_t valueOffset, uint64_t valueLen, uint64_t kvOffset, uint64_t kvLen)
{
    /* 当key长度大于6B时，设置type=HASH_AND_KEY_VALUE_ADDR，此时哈希表中存储的是key的hash值(6B)，以及KV对在file上的地址。
    此时在比较时需要从file上读取数据，但可以认为hash值冲突概率极小，需要去数据文件中读取KV值 */
    if(keyLen > MAPPING_TABLE_MAX_KEY_LEN)
    {
        this->type = HASH_AND_KEY_VALUE_ADDR;
        this->keyLen = MAPPING_TABLE_HASH_LEN;
        memcpy(this->hash, &hash, MAPPING_TABLE_HASH_LEN);
        
        this->valueLen = 0;
        this->addr.offset = kvOffset;
        this->addr.size = kvLen;
        return;
    }
    
    /* 当key长度小于等于6B，并且value长度大于8B时，设置type = KEY_AND_VALUE_ADDR，此时哈希表中存储的是key和value的地址。
    此时需要去数据文件中读value，优化大value下索引空间占用 */
    if(valueLen > MAPPING_TABLE_MAX_VALUE_LEN)
    {
        this->type = KEY_AND_VALUE_ADDR;
        this->keyLen = keyLen;
        memcpy(this->key, key, keyLen);
        
        this->valueLen = 0;
        this->valueAddr.offset = valueOffset;
        this->valueAddr.size = valueLen;
        return;
    }
    
    /* 当key长度小于等于6B，并且value长度小于等于8B时，设置type = KEY_AND_VALUE，KV直接存储在哈希表中。
    此时不需要去数据文件中读，优化小key小value下索引表读性能 */
    this->type = KEY_AND_VALUE;
    this->keyLen = keyLen;
    memcpy(this->key, key, keyLen);
    
    this->valueLen = valueLen;
    memcpy(this->value, value, valueLen);
}

uint64_t MappingTable::CalculHash(const void* buffer, size_t length)
{
    uint64_t const seed = 0;   /* or any other value */
    uint64_t hash = XXH64(buffer, length, seed);
    return hash;
}

Bucket *MappingTable::GetBucket(uint64_t hash)
{
    return &this->m_buckets[hash & (this->m_bucketsNum - 1)];
}

uint64_t MappingTable::AllocEntry()
{
    uint64_t entryIdx = atomic_fetch_add(&m_allocIdx, 1);
    return entryIdx < m_totalEntriesNum ? entryIdx : INVALID_ENTRY_NO;
}

void MappingTable::SetUpFileCtrl(FileCtrl *fileCtrl)
{
    this->m_fileCtrl = fileCtrl;
}

int32_t MappingTable::DoMmap()
{
    int fd;
    void *startAddr;
    off_t lseekOff;
    int64_t totalMemSize;
    
    // 1. 打开文件
    fd = open(this->m_memMappingFilePath.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
    if(-1 == fd)
    {
        cout << "open " << this->m_memMappingFilePath << " failed." << endl;
        return -1;
    }
    
    // 2.计算需要mapping的内存大小
    totalMemSize = this->m_bucketsNum * sizeof(Bucket) + this->m_totalEntriesNum * sizeof(Entry);
    
    // 3. 调整文件大小
    lseekOff = lseek(fd, (off_t)(totalMemSize - 1), SEEK_SET);
    if(-1 == lseekOff)
    {
        cout << "lseek " << this->m_memMappingFilePath << " failed, offset " << totalMemSize - 1 << endl;
        return -1;
    }
    write(fd, "\0", 1);
    lseek(fd, 0, SEEK_SET);
    
    // 4. mmap
    startAddr = mmap(0, totalMemSize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(startAddr == (void*)-1)
    {
        cout << "mmap " << this->m_memMappingFilePath << " failed, size " << totalMemSize << endl;
        return -1;
    }
    
    // 5. 关闭文件
    close(fd);
    this->m_startAddr = (uint8_t *)startAddr;
    this->m_buckets = (Bucket *)this->m_startAddr;
    this->m_entries = (Entry *)(this->m_startAddr + this->m_bucketsNum * sizeof(Bucket));
    this->m_fileLen = totalMemSize;
    
    // 6. 让系统分配哈希桶的物理内存
    memset(this->m_buckets, 0, this->m_bucketsNum * sizeof(Bucket));
    
    // 7. 锁定哈希桶内存不允许交换，等到索引表创建完成后再持久化到内存
    mlock(this->m_buckets, this->m_bucketsNum * sizeof(Bucket));
    
    return 0;
}

int32_t MappingTable::Put(uint8_t *key, uint64_t keyOffset, uint64_t keyLen, uint8_t *value, uint64_t valueOffset, uint64_t valueLen, uint64_t kvOffset, uint64_t kvLen)
{
    uint64_t hash;
    uint64_t entryIdx, headEntryIdx;
    Entry *entry;
    Bucket *bucket;
    entryIdx = AllocEntry();
    if(INVALID_ENTRY_NO == entryIdx)
    {
        cout << "alloc entry failed, idx " << this->m_allocIdx << endl;
        return -1;
    }
    
    hash = CalculHash(key, keyLen);
    entry = &m_entries[entryIdx];
    entry->Init(key, keyOffset, keyLen, value, valueOffset, valueLen, kvOffset, kvLen);
    bucket = GetBucket(hash);
    do
    {
        headEntryIdx = bucket->next_entry_idx;
        entry->next = headEntryIdx;
    }while(!__sync_bool_compare_and_swap(&bucket->next_entry_idx, headEntryIdx, entryIdx));
    
    return 0;
}

int32_t MappingTable::Get(uint8_t *key, uint64_t keyLen, uint8_t **outValue, uint64_t *outValueLen)
{
    uint64_t hash;
    Bucket *bucket;
    Entry *entry;
    bool found = false;
    
    uint8_t *tmpKey;
    uint64_t tmpKeyLen;
    uint8_t *tmpValue;
    uint64_t tmpValueLen;
    
    hash = CalculHash(key, keyLen);
    bucket = GetBucket(hash);
    entry = &m_entries[bucket->next_entry_idx];
    while(INVALID_ENTRY_NO != entry->next && !found)
    {
        switch (entry->type)
        {
            case KEY_AND_VALUE:
                found = (keyLen == entry->keyLen && 0 == memcmp(key, entry->key, keyLen));
                if(found)
                {
                    *outValue = entry->value;
                    *outValueLen = entry->valueLen;
                }
                break;
                
            case KEY_AND_VALUE_ADDR:
                found = (keyLen == entry->keyLen && 0 == memcmp(key, entry->key, keyLen));
                if(found)
                {
                    m_fileCtrl->ReadValue(entry->valueAddr.offset, entry->valueAddr.size, outValue);
                    *outValueLen = entry->valueAddr.size;
                }
                break;
                
            case HASH_AND_KEY_VALUE_ADDR:
                found = (0 == memcmp(&hash, entry->hash, entry->keyLen));
                if(found)
                {
                    m_fileCtrl->ReadKVAt(entry->addr.offset, &tmpKey, &tmpKeyLen, &tmpValue, &tmpValueLen);
                    if(keyLen == tmpKeyLen && 0 == memcmp(key, tmpKey, keyLen))
                    {
                        *outValue = tmpValue;
                        *outValueLen = tmpValueLen;
                    }
                    else
                    {
                        found = false;
                    }
                }
                break;
                
            default:
                break;
        }
        entry = &m_entries[entry->next];
    }
    
    return NULL == entry ? -1 : 0;
}

// 索引构建完成，持久化哈希桶内存
int32_t MappingTable::IndexBuildComplete()
{
    int fd;
    // 1. 打开文件
    fd = open(m_memMappingFilePath.c_str(), O_RDWR);
    if(-1 == fd)
    {
        cout << "open " << m_memMappingFilePath << " failed." << endl;
        return -1;
    }
    
    // 2. 将哈希桶持久化到文件中
    write(fd, m_buckets, this->m_bucketsNum * sizeof(Bucket));
    
    // 3. 关闭文件
    close(fd);
    
    return 0;
}


MappingTable::~MappingTable()
{
    if(NULL != this->m_startAddr)
    {
        munlock(this->m_buckets, this->m_bucketsNum * sizeof(Bucket));
        munmap(this->m_startAddr, this->m_fileLen);
    }
}
