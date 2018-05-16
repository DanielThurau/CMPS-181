
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return IX_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return IX_MALLOC_FAILED;
    newLeafBasedPage(firstPageData, -1, -1);

    // Adds the first record based page.
    IXFileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), *handle.fileHandle))
        return IX_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return IX_APPEND_FAILED;
    _pf_manager->closeFile(*handle.fileHandle);

    free(firstPageData);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), *ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(*ixfileHandle.fileHandle);
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


void IndexManager::newLeafBasedPage(void *page, int32_t leftSibling, int32_t rightSibling){
    memset(page, 0, PAGE_SIZE);
    
    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory) + sizeof(LeafDirectory);
    indexDirectory.type = LEAF_NODE;

    LeafDirectory leafDirectory;
    leafDirectory.leftSibling = leftSibling;
    leafDirectory.rightSibling = rightSibling;

    setIndexDirectory(page, indexDirectory);
    setLeafDirectory(page, leafDirectory);
}

void IndexManager::newInteriorBasedPage(void *page){
    memset(page, 0, PAGE_SIZE);

    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory);
    indexDirectory.type = INTERIOR_NODE;
    
    setIndexDirectory(page, indexDirectory);
}

void IndexManager::setIndexDirectory(void *page, IndexDirectory &directory) {
    memcpy(page, &directory, sizeof(directory));
}

void IndexManager::getIndexDirectory(const void *page, IndexDirectory &directory) {
    memcpy(&directory, page, sizeof(directory));
}

void IndexManager::setLeafDirectory(void *page, LeafDirectory &directory) {
    memcpy((uint8_t*) page + sizeof(IndexDirectory), &directory, sizeof(directory));
}

void IndexManager::getLeafDirectory(const void *page, LeafDirectory &directory) {
    memcpy(&directory, (uint8_t*) page + sizeof(IndexDirectory), sizeof(directory));
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

    fileHandle = new FileHandle();

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
    appendPageCount = ixAppendPageCounter;
    return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data){
    RC rc = fileHandle->readPage(pageNum, data);
    if(rc){
        return -1;
    }
    ixReadPageCounter++;
    return 0;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
    RC rc = fileHandle->writePage(pageNum, data);
    if(rc){
        return -1;
    }
    ixWritePageCounter++;
    return 0;
}   

RC IXFileHandle::appendPage(const void *data){
    RC rc = fileHandle->appendPage(data);
    if(rc){
        return -1;
    }
    ixAppendPageCounter++;
    return 0;
}   

unsigned IXFileHandle::getNumberOfPages(){
    return fileHandle->getNumberOfPages();
}

InteriorNode::InteriorNode(const void *page, Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    IndexDirectory directory;
    im->getIndexDirectory(page, directory);

    numEntries = directory.numEntries;
    freeSpaceOffset = directory.freeSpaceOffset;

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(directory);
    // copy page pointers out of page
    for (uint32_t i = 0; i < numEntries + 1; i++) {
        pagePointers.push_back( *((uint32_t*) cur_offset) );
        cur_offset += sizeof(uint32_t);
    }

    // copy traffic cops out of page
    for (uint32_t i = 0; i < numEntries; i++) {
        // alloc an extra byte so that adding the null plug to a varchar won't overflow
        void *value = malloc(attribute.length + 1);
        switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(value, cur_offset, attribute.length);
            trafficCops.push_back(value);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            uint32_t varchar_length;
            memcpy(&varchar_length, cur_offset, VARCHAR_LENGTH_SIZE);
            // move past length so length isn't included when we copy
            cur_offset += VARCHAR_LENGTH_SIZE;
            memcpy(value, cur_offset, varchar_length);
            // add null plug to varchar to make it into a c string
            ((char*) value)[varchar_length + 1] = '\0';
            cur_offset += varchar_length;
            trafficCops.push_back(value);
            break;
        }
    }
}

RC InteriorNode::writeToPage(void *page, Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    IndexDirectory directory;
    directory.type = INTERIOR_NODE;
    directory.numEntries = numEntries;
    directory.freeSpaceOffset = freeSpaceOffset;

    im->setIndexDirectory(page, directory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(directory);
    for (uint32_t i = 0; i < numEntries + 1; i++) {
        memcpy(cur_offset, &pagePointers[i], sizeof(uint32_t));
        cur_offset += sizeof(uint32_t);
    }

    for (uint32_t i = 0; i < numEntries; i++) {
        switch(attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(cur_offset, trafficCops[i], attribute.length);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            // NOTE this is memccpy, not memcpy
            // copy from trafficCops entry until we hit the null plug
            // memccpy returns a pointer to the last thing copied, so get length by subtracting the start from that
            void *varchar_end = memccpy(cur_offset + VARCHAR_LENGTH_SIZE, trafficCops[i], '\0', attribute.length);
            uint32_t varchar_length = (uint8_t*) varchar_end - cur_offset + VARCHAR_LENGTH_SIZE;
            memcpy(cur_offset, &varchar_length, VARCHAR_LENGTH_SIZE);
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }
    return SUCCESS;
}


LeafNode::LeafNode(const void *page, Attribute &attribute){
    IndexManager *im = IndexManager::instance();

    IndexDirectory indexDirectory;
    im->getIndexDirectory(page, indexDirectory);

    numEntries = indexDirectory.numEntries;
    freeSpaceOffset = indexDirectory.freeSpaceOffset;

    // set the leafDirectory public variable
    im->getLeafDirectory(page, siblings);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(siblings);

    // copy traffic cops out of page
    for (uint32_t i = 0; i < numEntries; i++) {
        // alloc an extra byte so that adding the null plug to a varchar won't overflow
        void *value = malloc(attribute.length + 1);
        switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(value, cur_offset, attribute.length);
            keys.push_back(value);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            uint32_t varchar_length;
            memcpy(&varchar_length, cur_offset, VARCHAR_LENGTH_SIZE);
            // move past length so length isn't included when we copy
            cur_offset += VARCHAR_LENGTH_SIZE;
            memcpy(value, cur_offset, varchar_length);
            // add null plug to varchar to make it into a c string
            ((char*) value)[varchar_length + 1] = '\0';
            keys.push_back(value);
            cur_offset += varchar_length;
            break;
        }
    }

    for (uint32_t i = 0; i < numEntries; i++) {
        rids.push_back( *((RID*) cur_offset) );
        cur_offset += sizeof(RID);
    }

}

RC LeafNode::writeToPage(void *page, Attribute &attribute){
    IndexManager *im = IndexManager::instance();

    IndexDirectory indexDirectory;
    indexDirectory.type = INTERIOR_NODE;
    indexDirectory.numEntries = numEntries;
    indexDirectory.freeSpaceOffset = freeSpaceOffset;

    im->setIndexDirectory(page, indexDirectory);
    im->setLeafDirectory(page, siblings);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(siblings);

    for (uint32_t i = 0; i < numEntries; i++) {
        switch(attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(cur_offset, keys[i], attribute.length);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            // NOTE this is memccpy, not memcpy
            // copy from trafficCops entry until we hit the null plug
            // memccpy returns a pointer to the last thing copied, so get length by subtracting the start from that
            void *varchar_end = memccpy(cur_offset + VARCHAR_LENGTH_SIZE, keys[i], '\0', attribute.length);
            uint32_t varchar_length = (uint8_t*) varchar_end - cur_offset + VARCHAR_LENGTH_SIZE;
            memcpy(cur_offset, &varchar_length, VARCHAR_LENGTH_SIZE);
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    for (uint32_t i = 0; i < numEntries; i++) {
        memcpy(cur_offset, &rids[i], sizeof(RID));
        cur_offset += sizeof(RID);
    }

    return SUCCESS;
}