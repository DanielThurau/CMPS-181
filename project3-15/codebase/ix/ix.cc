
#include "ix.h"

#include <cmath>
#include <iostream>
#include <algorithm>

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
    void *page = malloc(PAGE_SIZE);
    PageNum pageNum;
    findPageWithKey(ixfileHandle, key, attribute, page, pageNum);
    LeafNode *node = new LeafNode(page, attribute);
    if (canEntryFitInLeafNode(*node, key, attribute)) {
        addEntryToLeafNode(*node, key, rid, attribute);
        node->writeToPage(page, attribute);
        ixfileHandle.writePage(pageNum, page);
    }

    free(page);
    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    PageNum pageNum;
    findPageWithKey(ixfileHandle, key, attribute, page, pageNum);
    LeafNode *node = new LeafNode(page, attribute);
    // iterate through each key and rid pair in the node
    for (size_t i = 0; i < node->keys.size(); i++) {
        // if this key / rid pair matches the one supplied
        if (compareAttributeValues(node->keys[i], key, attribute) == 0 &&
            node->rids[i].pageNum == rid.pageNum && node->rids[i].slotNum == rid.slotNum) {
            // remove it
            node->keys.erase(node->keys.begin() + i);
            node->rids.erase(node->rids.begin() + i);
            node->writeToPage(page, attribute);
            ixfileHandle.writePage(pageNum, page);
            delete node;
            return SUCCESS;
        }
    }
    delete node;
    return IX_KEY_NOT_FOUND;
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
    cout << "{" << endl;
    printTreeRecur(ixfileHandle, attribute, 0, 0);
    cout << endl << "}" << endl;
}


void IndexManager::printTreeRecur(IXFileHandle &ixfileHandle, const Attribute &attribute, PageNum pageNum, int depth) const {
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    if (getNodeType(page) == LEAF_NODE) {
        LeafNode *node = new LeafNode(page, attribute);
        printLeafNode(ixfileHandle, attribute, *node, depth);
        delete node;
    }
    else {
        InteriorNode *node = new InteriorNode(page, attribute);
        printInteriorNode(ixfileHandle, attribute, *node, depth);
        delete node;
    }
}

void IndexManager::printInteriorNode(IXFileHandle &ixfileHandle, const Attribute &attribute, InteriorNode &node, int depth) const {
    string spaces = string(depth * 4, ' ');
    cout << "\"keys\":[";
    for (size_t i = 0; i < node.trafficCops.size(); i++) {
        cout << "\"";
        printKey(node.trafficCops[i], attribute);
        cout << "\"";
        if (i < node.trafficCops.size() - 1) {
            cout << ",";
        }
    }
    cout << "]," << endl;
    cout << spaces << "\"children\":[" << endl;
    for (size_t i = 0; i < node.pagePointers.size(); i++) {
        cout << string((depth + 1) * 4 - 1, ' ') << "{";
        printTreeRecur(ixfileHandle, attribute, node.pagePointers[i], depth + 1);
        if (i < node.pagePointers.size() - 1) {
            cout << "}," << endl;
        }
        else {
            cout << "}" << endl;
        }
    }
    cout << spaces << "]";
}

void IndexManager::printLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute, LeafNode &node, int depth) const {
    cout << "\"keys\": [";
    for (size_t i = 0; i < node.keys.size();) {
        cout << "\"";
        printKey(node.keys[i], attribute);
        cout << ":[";
        void *cur_key = node.keys[i];
        for (;;) {
            cout << "(" << node.rids[i].pageNum << "," << node.rids[i].slotNum << ")";
            i++;
            if (i < node.keys.size() && compareAttributeValues(cur_key, node.keys[i], attribute) == 0) {
                cout << ",";
            }
            else break;
        }
        if (i < node.keys.size() - 1) {
            cout << "]\",";
        }
        else {
            cout << "]\"";
        }
    }
    cout << "]";
}

void IndexManager::printKey(void *key, const Attribute &attribute) const {
    switch(attribute.type) {
    case TypeInt:
        cout << *((unsigned *) key);
        break;
    case TypeReal:
        cout << *((float *) key);
        break;
    case TypeVarChar: {
        uint32_t varchar_size;
        memcpy(&varchar_size, key, VARCHAR_LENGTH_SIZE);
        char varchar_str[varchar_size + 1];
        memcpy(varchar_str, (uint8_t *) key + VARCHAR_LENGTH_SIZE, varchar_size);
        varchar_str[varchar_size] = '\0';
        cout << varchar_str;
    }
    }
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

void IndexManager::getIndexDirectory(const void *page, IndexDirectory &directory) const {
    memcpy(&directory, page, sizeof(directory));
}

NodeType IndexManager::getNodeType(const void *page) const {
    IndexDirectory directory;
    getIndexDirectory(page, directory);
    return directory.type;
}

int IndexManager::compareAttributeValues(const void *key_1, const void *key_2, const Attribute &attribute) const {
    switch (attribute.type) {
    case TypeInt:
        return *((unsigned *) key_1) - *((unsigned *) key_2);
    case TypeReal: {
        float fkey_1 = *((float*) key_1);
        float fkey_2 = *((float*) key_2);
        // do float comparisons explicitly to avoid rounding errors
        if (fkey_1 == fkey_2) return 0;
        if (fkey_1 < fkey_2) return -1;
        if (fkey_1 > fkey_2) return 1;
    }
    case TypeVarChar:  {
        uint32_t len_1;
        uint32_t len_2;
        memcpy(&len_1, key_1, VARCHAR_LENGTH_SIZE);
        memcpy(&len_2, key_2, VARCHAR_LENGTH_SIZE);
        char key_1_str[len_1 + 1];
        char key_2_str[len_2 + 1];
        memcpy(key_1_str, (uint8_t *) key_1 + VARCHAR_LENGTH_SIZE, len_1);
        memcpy(key_2_str, (uint8_t *) key_2 + VARCHAR_LENGTH_SIZE, len_2);
        key_1_str[len_1] = '\0';
        key_2_str[len_2] = '\0';
        return strcmp(key_1_str, key_2_str);
    }
    }
    // should never reach here, but do this so compiler doesn't complain
    return 0;
}

void IndexManager::findPageWithKey(IXFileHandle &ixfileHandle, const void *key, const Attribute &attribute, void *page, PageNum &pageNum) {
    pageNum = 0;
    ixfileHandle.readPage(pageNum, page);
    while (getNodeType(page) != LEAF_NODE) {
        InteriorNode *node = new InteriorNode(page, attribute);
        uint32_t i = 0;
        for (; i < node->indexDirectory.numEntries; i++) {
            if (compareAttributeValues(key, node->trafficCops[i], attribute) < 0) {
                pageNum = node->pagePointers[i];
                break;
            }
        }
        if (i == node->indexDirectory.numEntries) {
            pageNum = node->pagePointers[node->indexDirectory.numEntries];
        }
        ixfileHandle.readPage(pageNum, page);
        delete node;
    }
}

bool IndexManager::canEntryFitInLeafNode(LeafNode node, const void *key, const Attribute &attribute) {
    uint32_t key_size;
    switch(attribute.type) {
    case TypeInt:
    case TypeReal:
        key_size = attribute.length;
        break;
    case TypeVarChar:
        uint32_t varchar_length;
        memcpy(&varchar_length, key, VARCHAR_LENGTH_SIZE);
        key_size = varchar_length + VARCHAR_LENGTH_SIZE;
        break;
    }
    return PAGE_SIZE - node.indexDirectory.freeSpaceOffset > key_size + sizeof(RID);
}

RC IndexManager::addEntryToLeafNode(LeafNode &node, const void *key, const RID rid, const Attribute &attribute) {
    uint32_t key_size;
    switch(attribute.type) {
    case TypeInt:
    case TypeReal:
        key_size = attribute.length;
        break;
    case TypeVarChar:
        uint32_t varchar_length;
        memcpy(&varchar_length, key, VARCHAR_LENGTH_SIZE);
        key_size = varchar_length + VARCHAR_LENGTH_SIZE;
        break;
    }
    void *key_cpy = malloc(key_size);
    memcpy(key_cpy, key, key_size);

    // find index to do inserted index at
    // a masterpiece
    auto cmp_func = [&](const void *a, const void *b){ return compareAttributeValues(a, b, attribute) < 0; };
    int insertion_index = distance(
        node.keys.begin(),
        upper_bound(node.keys.begin(), node.keys.end(), key, cmp_func)
    );

    node.keys.insert(node.keys.begin() + insertion_index, key_cpy);
    node.rids.insert(node.rids.begin() + insertion_index, rid);
    node.indexDirectory.freeSpaceOffset += key_size + sizeof(RID);
    node.indexDirectory.numEntries++;
    return SUCCESS;
}

void IndexManager::setFamilyDirectory(void *page, FamilyDirectory &directory) {
    memcpy((uint8_t*) page + sizeof(IndexDirectory), &directory, sizeof(directory));
}

void IndexManager::getFamilyDirectory(const void *page, FamilyDirectory &directory) {
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
        void *value = malloc(attribute.length);
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
            trafficCops.push_back(value);
            cur_offset += varchar_length + VARCHAR_LENGTH_SIZE;
            break;
        }
    }
}

RC InteriorNode::writeToPage(void *page, const Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    im->setIndexDirectory(page, indexDirectory);
    im->setFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);
    for (uint32_t i = 0; i < indexDirectory.numEntries + 1; i++) {
        memcpy(cur_offset, &pagePointers[i], sizeof(PageNum));
        cur_offset += sizeof(PageNum);
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
        void *value = malloc(attribute.length);
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

RC LeafNode::writeToPage(void *page, const Attribute &attribute){
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