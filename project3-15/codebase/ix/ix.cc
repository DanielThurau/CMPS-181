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
    void *page = malloc(PAGE_SIZE);
    PageNum pageNum;
    findPageWithKey(ixfileHandle, key, attribute, page, pageNum);
    LeafNode *node = new LeafNode(page, attribute, pageNum);
    if (canEntryFitInLeafNode(*node, key, attribute)) {
        addEntryToLeafNode(*node, key, rid, attribute);
        node->writeToPage(page, attribute);
        ixfileHandle.writePage(pageNum, page);
    }else{

    }
    // else split and insert into page

    free(page);
    return SUCCESS;
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
    cout << "{" << endl;
    printTreeRecur(ixfileHandle, attribute, 0, 0);
    cout << endl << "}" << endl;
}


void IndexManager::printTreeRecur(IXFileHandle &ixfileHandle, const Attribute &attribute, PageNum pageNum, int depth) const {
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    if (getNodeType(page) == LEAF_NODE) {
        LeafNode *node = new LeafNode(page, attribute, pageNum);
        printLeafNode(ixfileHandle, attribute, *node, depth + 1);
        delete node;
    }
    else {
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);
        printInteriorNode(ixfileHandle, attribute, *node, depth + 1);
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
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);
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

bool IndexManager::canEntryFitInInteriorNode(InteriorNode node, const void *key, const Attribute &attribute) {
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
    return PAGE_SIZE - node.indexDirectory.freeSpaceOffset > key_size + sizeof(PageNum);
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

RC IndexManager::addEntryToInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &node, const void *key, const Attribute &attribute) {
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
        node.trafficCops.begin(),
        upper_bound(node.trafficCops.begin(), node.trafficCops.end(), key, cmp_func)
    );

    node.trafficCops.insert(node.trafficCops.begin() + insertion_index, key_cpy);
    node.pagePointers.insert(node.pagePointers.begin() + insertion_index, ixfileHandle.getNumberOfPages() - 1);
    node.indexDirectory.freeSpaceOffset += key_size + sizeof(PageNum);
    node.indexDirectory.numEntries++;
    return SUCCESS;
}


void IndexManager::setFamilyDirectory(void *page, FamilyDirectory &directory) {
    memcpy((uint8_t*) page + sizeof(IndexDirectory), &directory, sizeof(directory));
}

void IndexManager::getFamilyDirectory(const void *page, FamilyDirectory &directory) {
    memcpy(&directory, (uint8_t*) page + sizeof(IndexDirectory), sizeof(directory));
}

RC IndexManager::insertAndSplit(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID rid, void *page, PageNum &pageNum){
    RC rc;
    if(getNodeType(page) == LEAF_NODE) {
        // load page data into leafNode object
        LeafNode *node = new LeafNode(page, attribute, pageNum);

        if(pageNum != 0){
            // add to vector even if it over fills the FSO
            if((rc = addEntryToLeafNode(*node, key, rid, attribute)) != SUCCESS){
                return -1;
            }

            // reference to empty leafNode object that will be split into
            LeafNode *newLeaf = new LeafNode();
            
            // newKey is value to be inserted into parent node
            void *newKey = splitLeafNode(ixfileHandle, *node, *newLeaf, attribute);

            // read in parent page 
            void *parentPage = malloc(PAGE_SIZE); 
            ixfileHandle.readPage(node->familyDirectory.parent, parentPage);

            // recursive call to insert into parent
            return insertAndSplit(ixfileHandle, attribute, newKey, rid, parentPage, node->familyDirectory.parent);
        }else{
            
        }
        

    }else{
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);

        if (canEntryFitInInteriorNode(*node, key, attribute)) {
            addEntryToInteriorNode(ixfileHandle, *node, key, attribute);
            return SUCCESS;
        }else{
            if(pageNum != 0){
                addEntryToInteriorNode(ixfileHandle, *node, key, attribute);

                InteriorNode *newNode = new InteriorNode();

                void *newKey = splitInteriorNode(ixfileHandle, *node, *newNode, attribute);

                void *parentPage = malloc(PAGE_SIZE);
                ixfileHandle.readPage(node->familyDirectory.parent, parentPage);

                return insertAndSplit(ixfileHandle, attribute, newKey, rid, parentPage, node->familyDirectory.parent);

            }
        }


    }
    return -1;
}

void *IndexManager::splitLeafNode(IXFileHandle &ixfileHandle, LeafNode &originLeaf, LeafNode &newLeaf, const Attribute &attribute){

    //set obvious directory values
    newLeaf.indexDirectory.type = LEAF_NODE;

    newLeaf.familyDirectory.leftSibling = originLeaf.selfPageNum;
    newLeaf.familyDirectory.rightSibling = originLeaf.familyDirectory.rightSibling;
    newLeaf.familyDirectory.parent = originLeaf.familyDirectory.parent;

    // pageNum is new page (need of non heap free page buffer manager)
    newLeaf.selfPageNum = ixfileHandle.getNumberOfPages();

    // take the subvector of the original node's keys and copy it to the new node
    vector<void*>::const_iterator firstKey = originLeaf.keys.begin() + ceil(originLeaf.keys.size() / 2) + 1;
    vector<void*>::const_iterator lastKey = originLeaf.keys.begin() + originLeaf.keys.size();
    newLeaf.keys = vector<void*>(firstKey, lastKey);

    // erase split values out of original node
    originLeaf.keys.erase(originLeaf.keys.begin() + ceil(originLeaf.keys.size()/2) + 1, originLeaf.keys.begin() + originLeaf.keys.size());

    // take the subvector of the original node's rids and copy it to the new node
    vector<RID>::const_iterator firstRID = originLeaf.rids.begin() + ceil(originLeaf.rids.size() / 2) + 1;
    vector<RID>::const_iterator lastRID = originLeaf.rids.begin() + originLeaf.rids.size();
    newLeaf.rids = vector<RID>(firstRID, lastRID);

    // erase split values out of original node
    originLeaf.rids.erase(originLeaf.rids.begin() + ceil(originLeaf.rids.size()/2) + 1, originLeaf.rids.begin() + originLeaf.rids.size());

    originLeaf.indexDirectory.numEntries = originLeaf.keys.size();
    newLeaf.indexDirectory.numEntries = originLeaf.keys.size();

    // recalculate Free space offset (difficult if varchar)
    originLeaf.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(originLeaf, attribute);
    newLeaf.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(newLeaf, attribute);

    void *page = malloc(PAGE_SIZE);

    // write nodes to new page and add to FS
    originLeaf.writeToPage(page, attribute);
    ixfileHandle.writePage(originLeaf.selfPageNum, page);

    // write nodes to new page and add to FS
    newLeaf.writeToPage(page, attribute);
    ixfileHandle.appendPage(page);

    // traffic cop is first key of new node (right sibling of origin)
    void* trafficCop = malloc(attribute.length);
    memcpy(trafficCop, newLeaf.keys[0], attribute.length);

    return trafficCop;
}

void *IndexManager::splitInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &originNode, InteriorNode &newNode, const Attribute &attribute){

    //set obvious directory values
    newNode.indexDirectory.type = INTERIOR_NODE;

    newNode.familyDirectory.leftSibling = originNode.selfPageNum;
    newNode.familyDirectory.rightSibling = originNode.familyDirectory.rightSibling;
    newNode.familyDirectory.parent = originNode.familyDirectory.parent;

    // pageNum is new page (need of non heap free page buffer manager)
    newNode.selfPageNum = ixfileHandle.getNumberOfPages();

    // take the subvector of the original node's keys and copy it to the new node
    vector<void*>::const_iterator firstKey = originNode.trafficCops.begin() + ceil(originNode.trafficCops.size() / 2) + 1;
    vector<void*>::const_iterator lastKey = originNode.trafficCops.begin() + originNode.trafficCops.size();
    newNode.trafficCops = vector<void*>(firstKey, lastKey);

    // erase split values out of original node
    originNode.trafficCops.erase(originNode.trafficCops.begin() + ceil(originNode.trafficCops.size()/2) + 1, originNode.trafficCops.begin() + originNode.trafficCops.size());

    // take the subvector of the original node's rids and copy it to the new node
    vector<PageNum>::const_iterator firstRID = originNode.pagePointers.begin() + ceil(originNode.pagePointers.size() / 2) + 1;
    vector<PageNum>::const_iterator lastRID = originNode.pagePointers.begin() + originNode.pagePointers.size();
    newNode.pagePointers = vector<PageNum>(firstRID, lastRID);

    // erase split values out of original node
    originNode.pagePointers.erase(originNode.pagePointers.begin() + ceil(originNode.pagePointers.size()/2) + 1, originNode.pagePointers.begin() + originNode.pagePointers.size());

    originNode.indexDirectory.numEntries = originNode.trafficCops.size();
    newNode.indexDirectory.numEntries = originNode.trafficCops.size();

    // recalculate Free space offset (difficult if varchar)
    originNode.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(originNode, attribute);
    newNode.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(newNode, attribute);

    // traffic cop is first key of new node (right sibling of origin)
    void* trafficCop = malloc(attribute.length);
    memcpy(trafficCop, newNode.trafficCops[0], attribute.length);

    newNode.trafficCops.erase(newNode.trafficCops.begin());
    newNode.pagePointers.erase(newNode.pagePointers.begin());

    void *page = malloc(PAGE_SIZE);

    // write nodes to new page and add to FS
    originNode.writeToPage(page, attribute);
    ixfileHandle.writePage(originNode.selfPageNum, page);

    // write nodes to new page and add to FS
    newNode.writeToPage(page, attribute);
    ixfileHandle.appendPage(page);


    return trafficCop;
}

uint32_t IndexManager::calculateFreeSpaceOffset(LeafNode &node, const Attribute &attribute){
    uint32_t FSO = 0;

    FSO += sizeof(FamilyDirectory);
    FSO += sizeof(IndexDirectory);

    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            FSO += 4 * node.keys.size(); 
            break;
        case TypeVarChar:
            for(unsigned i = 0; i < node.keys.size(); i++){
                uint32_t varchar_length;
                memcpy(&varchar_length, node.keys[i], VARCHAR_LENGTH_SIZE);
                FSO += varchar_length + VARCHAR_LENGTH_SIZE;
            }
            break;
        }

    FSO += sizeof(RID) * node.rids.size();

    return FSO;
}

uint32_t IndexManager::calculateFreeSpaceOffset(InteriorNode &node, const Attribute &attribute){
    uint32_t FSO = 0;

    FSO += sizeof(FamilyDirectory);
    FSO += sizeof(IndexDirectory);

    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            FSO += 4 * node.trafficCops.size(); 
            break;
        case TypeVarChar:
            for(unsigned i = 0; i < node.trafficCops.size(); i++){
                uint32_t varchar_length;
                memcpy(&varchar_length, node.trafficCops[i], VARCHAR_LENGTH_SIZE);
                FSO += varchar_length + VARCHAR_LENGTH_SIZE;
            }
            break;
        }

    FSO += sizeof(PageNum) * node.pagePointers.size();

    return FSO;
}


RC IndexManager::insertIntoInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID rid, void *page){
    return -1;
}

RC IndexManager::insertIntoLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, void *page){
    return -1;
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

InteriorNode::InteriorNode(){}

InteriorNode::InteriorNode(const void *page, const Attribute &attribute, PageNum pageNum) {
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);
    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    selfPageNum = pageNum;

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

LeafNode::LeafNode(){}

LeafNode::LeafNode(const void *page, const Attribute &attribute, PageNum pageNum){
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);

    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    selfPageNum = pageNum;

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