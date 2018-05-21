
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
    // create the root
    newLeafBasedPage(firstPageData, -1, -1, 0);

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


void IndexManager::newLeafBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent){
    memset(page, 0, PAGE_SIZE);
    
    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory) + sizeof(FamilyDirectory);
    indexDirectory.type = LEAF_NODE;

    FamilyDirectory familyDirectory;
    familyDirectory.parent = parent;    
    familyDirectory.leftSibling = leftSibling;
    familyDirectory.rightSibling = rightSibling;

    setIndexDirectory(page, indexDirectory);
    setFamilyDirectory(page, familyDirectory);
}

void IndexManager::newInteriorBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent){
    memset(page, 0, PAGE_SIZE);

    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory);
    indexDirectory.type = INTERIOR_NODE;

    FamilyDirectory familyDirectory;
    familyDirectory.parent = parent;    
    familyDirectory.leftSibling = leftSibling;
    familyDirectory.rightSibling = rightSibling;

    setIndexDirectory(page, indexDirectory);
    setFamilyDirectory(page, familyDirectory);
}

void IndexManager::setIndexDirectory(void *page, IndexDirectory &directory) {
    memcpy(page, &directory, sizeof(directory));
}

void IndexManager::getIndexDirectory(const void *page, IndexDirectory &directory) {
    memcpy(&directory, page, sizeof(directory));
}

void IndexManager::setFamilyDirectory(void *page, FamilyDirectory &directory) {
    memcpy((uint8_t*) page + sizeof(IndexDirectory), &directory, sizeof(directory));
}

void IndexManager::getFamilyDirectory(const void *page, FamilyDirectory &directory) {
    memcpy(&directory, (uint8_t*) page + sizeof(IndexDirectory), sizeof(directory));
}

RC IndexManager::insertAndSplit(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *trafficCop, void *page){
    // first time we call this function
    if(trafficCop == nullptr){
        LeafNode leafNode = LeafNode(page, attribute);
        
    }
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

InteriorNode::InteriorNode(const void *page, const Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);
    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);
    // copy page pointers out of page
    for (uint32_t i = 0; i < indexDirectory.numEntries + 1; i++) {
        pagePointers.push_back( *((uint32_t*) cur_offset) );
        cur_offset += sizeof(uint32_t);
    }

    // copy traffic cops out of page
    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
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
            memcpy(value, cur_offset, varchar_length + VARCHAR_LENGTH_SIZE);
            // add null plug to varchar to make it into a c string
            trafficCops.push_back(value);
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }
}

RC InteriorNode::writeToPage(void *page, Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    im->setIndexDirectory(page, indexDirectory);
    im->setFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);
    for (uint32_t i = 0; i < indexDirectory.numEntries + 1; i++) {
        memcpy(cur_offset, &pagePointers[i], sizeof(uint32_t));
        cur_offset += sizeof(uint32_t);
    }

    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
        switch(attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(cur_offset, trafficCops[i], attribute.length);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            uint32_t varchar_length;
            memcpy(&varchar_length, trafficCops[i], VARCHAR_LENGTH_SIZE);
            memcpy(cur_offset, trafficCops[i], varchar_length + VARCHAR_LENGTH_SIZE);
            // add null plug to varchar to make it into a c string
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }
    return SUCCESS;
}


LeafNode::LeafNode(const void *page, const Attribute &attribute){
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);

    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);

    // copy traffic cops out of page
    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
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
            memcpy(value, cur_offset, varchar_length + VARCHAR_LENGTH_SIZE);
            // add null plug to varchar to make it into a c string
            keys.push_back(value);
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
        rids.push_back( *((RID*) cur_offset) );
        cur_offset += sizeof(RID);
    }

}

RC LeafNode::writeToPage(void *page, Attribute &attribute){
    IndexManager *im = IndexManager::instance();

    im->setIndexDirectory(page, indexDirectory);
    im->setFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);

    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
        switch(attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy(cur_offset, keys[i], attribute.length);
            cur_offset += attribute.length;
            break;
        case TypeVarChar:
            uint32_t varchar_length;
            memcpy(&varchar_length, keys[i], VARCHAR_LENGTH_SIZE);
            memcpy(cur_offset, keys[i], varchar_length + VARCHAR_LENGTH_SIZE);
            // add null plug to varchar to make it into a c string
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    for (uint32_t i = 0; i < indexDirectory.numEntries; i++) {
        memcpy(cur_offset, &rids[i], sizeof(RID));
        cur_offset += sizeof(RID);
    }

    return SUCCESS;
}