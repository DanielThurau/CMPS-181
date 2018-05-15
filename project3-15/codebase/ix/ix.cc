
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;

    FileHandle *fileHandle = new FileHandle();

}

IXFileHandle::~IXFileHandle()
{
    fileHandle->~FileHandle();
    free(fileHandle);
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPage = ixAppendPageCounter;
    return success;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){
    RC rc = fileHandle->readPage(pageNum, data);
    if(rc){
        return -1;
    }
    ixReadPageCounter++;
    return success;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
    RC rc = fileHandle->writePage(pageNum, data);
    if(rc){
        return -1;
    }
    ixWritePageCounter++;
    return success;
}   

RC IXFileHandle::appendPage(const void *data){
    RC rc = fileHandle->appendPage(pageNum, data);
    if(rc){
        return -1;
    }
    ixAppendPageCounter++;
    return success;
}   

unsigned IXFileHandle::getNumberOfPages(){
    return fileHandle->getNumberOfPages();
}