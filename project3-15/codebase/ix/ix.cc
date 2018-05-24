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

// Creates a new file using the index manager's ixfileHandle
// and places a leaf node at the root
RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return IX_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = malloc(PAGE_SIZE);
    if (firstPageData == NULL)
        return IX_MALLOC_FAILED;
    
    // create the root as a leaf
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
    RC rc = _pf_manager->openFile(fileName.c_str(), *ixfileHandle.fileHandle);
    if (rc == SUCCESS) {
        ixfileHandle.opened = true;
}
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    RC rc = _pf_manager->closeFile(*ixfileHandle.fileHandle);
    if (rc == SUCCESS) {
        ixfileHandle.opened = false;
    }
    return rc;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    if (page == NULL)
        return IX_MALLOC_FAILED;

    PageNum pageNum;
    // return a page with populated data and pageNum that contains the key
    findPageWithKey(ixfileHandle, key, attribute, page, pageNum);
    
    // Leaf node object populated by the return data
    LeafNode *node = new LeafNode(page, attribute, pageNum);

    // Can the <key,rid> pair fit into the current page
    if (canEntryFitInLeafNode(*node, key, attribute)) {

        // add entry into object representation
        if(addEntryToLeafNode(*node, key, rid, attribute))
            return IM_ADD_ENTRY_FAILED;
        
        // write the 
        if(node->writeToPage(page, attribute))
            return LN_WRITE_FAILED;
        
        ixfileHandle.writePage(pageNum, page);
    }else{
        // recursively call method that splits nodes    
        if(insertAndSplit(ixfileHandle, attribute, key, rid, page, pageNum))
            return IM_SPLIT_FAILED;
    }

    free(page);
    delete node;

    return SUCCESS;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void *page = malloc(PAGE_SIZE);
    PageNum pageNum;
    findPageWithKey(ixfileHandle, key, attribute, page, pageNum);
    LeafNode *node = new LeafNode(page, attribute, pageNum);
    // iterate through each key and rid pair in the node
    for (size_t i = 0; i < node->keys.size(); i++) {
        // if this key / rid pair matches the one supplied
        if (compareAttributeValues(node->keys[i], key, attribute) == 0 &&
            node->rids[i].pageNum == rid.pageNum && node->rids[i].slotNum == rid.slotNum) {
            // remove it
            node->keys.erase(node->keys.begin() + i);
            node->rids.erase(node->rids.begin() + i);
            node->indexDirectory.numEntries--;
            node->indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(*node);

            node->writeToPage(page, attribute);
            ixfileHandle.writePage(pageNum, page);
            delete node;
            free(page);
            return SUCCESS;
        }
    }
    delete node;
    free(page);
    return IM_KEY_NOT_FOUND;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    if (!ixfileHandle.opened) {
        return IX_FILE_NOT_OPENED;
    }

    return ix_ScanIterator.init(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

// top level print just calls printTreeRecur on page 0, with depth 0
void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    cout << "{" << endl;
    printTreeRecur(ixfileHandle, attribute, 0, 0);
    cout << endl << "}" << endl;
}

// recursively print an index tree
void IndexManager::printTreeRecur(IXFileHandle &ixfileHandle, const Attribute &attribute, PageNum pageNum, int depth) const {
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.readPage(pageNum, page);
    // gets the node type of the given page and calls the appropriate node printing function
    if (getNodeType(page) == LEAF_NODE) {
        LeafNode *node = new LeafNode(page, attribute, pageNum);
        printLeafNode(ixfileHandle, attribute, *node, depth);
        delete node;
    }
    else {
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);
        printInteriorNode(ixfileHandle, attribute, *node, depth);
        delete node;
    }
    free(page);
}

// print an interior node and all of its children
void IndexManager::printInteriorNode(IXFileHandle &ixfileHandle, const Attribute &attribute, InteriorNode &node, int depth) const {
    // used for indenting
    string spaces = string(depth * 4, ' ');
    // print keys
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
    // print children
    cout << spaces << "\"children\":[" << endl;
    for (size_t i = 0; i < node.pagePointers.size(); i++) {
        cout << string((depth + 1) * 4 - 1, ' ') << "{";
        // call printTreeRecur on all children
        // very unlikely that this will cause a stack overflow since tree is almost always only be a few levels deep
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

// print a leaf node
void IndexManager::printLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute, LeafNode &node, int depth) const {
    cout << "\"keys\": [";
    for (size_t i = 0; i < node.keys.size();) {
        cout << "\"";
        // print this key
        printKey(node.keys[i], attribute);
        cout << ":[";
        void *cur_key = node.keys[i];
        // loop to find repeated keys so RIDs for the same key can be printed together
        for (;;) {
            // print RID
            cout << "(" << node.rids[i].pageNum << "," << node.rids[i].slotNum << ")";
            i++;
            // if this isn't the end of the keys array and the next value is the same as the previous one, keep going
            if (i < node.keys.size() && compareAttributeValues(cur_key, node.keys[i], attribute) == 0) {
                cout << ",";
            }
            else break;
        }
        if (i < node.keys.size()) {
            cout << "]\",";
        }
        else {
            cout << "]\"";
        }
    }
    cout << "]";
}

// print a key
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

// Create an empty leaf based page with directory initialized
void IndexManager::newLeafBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent){
    memset(page, 0, PAGE_SIZE);
    
    // IndexDirectory Structure (ix.h)
    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory) + sizeof(FamilyDirectory);
    indexDirectory.type = LEAF_NODE;

    // FamilyDirectory Strucutre (ix.h)
    FamilyDirectory familyDirectory;
    familyDirectory.parent = parent;    
    familyDirectory.leftSibling = leftSibling;
    familyDirectory.rightSibling = rightSibling;

    // Given page set the specific directory
    setIndexDirectory(page, indexDirectory);
    setFamilyDirectory(page, familyDirectory);
}

// Create an empty leaf based page with the directory initialized
void IndexManager::newInteriorBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent){
    memset(page, 0, PAGE_SIZE);

    IndexDirectory indexDirectory;
    indexDirectory.numEntries = 0;
    indexDirectory.freeSpaceOffset = sizeof(IndexDirectory) + sizeof(FamilyDirectory);
    indexDirectory.type = INTERIOR_NODE;

    FamilyDirectory familyDirectory;
    familyDirectory.parent = parent;    
    familyDirectory.leftSibling = leftSibling;
    familyDirectory.rightSibling = rightSibling;

    setIndexDirectory(page, indexDirectory);
    setFamilyDirectory(page, familyDirectory);
}

// Given a malloc'd page, set the directory at the correct location
void IndexManager::setIndexDirectory(void *page, IndexDirectory &directory) {
    memcpy(page, &directory, sizeof(directory));
}

// Given a malloc'd page, get the directory of the page and populate the given struct
void IndexManager::getIndexDirectory(const void *page, IndexDirectory &directory) const {
    memcpy(&directory, page, sizeof(directory));
}

// Given a malloc'd page, set the directory at the correct location
void IndexManager::setFamilyDirectory(void *page, FamilyDirectory &directory) {
    memcpy((uint8_t*) page + sizeof(IndexDirectory), &directory, sizeof(directory));
}

// Given a malloc'd page, get the directory of the page and populate the given struct
void IndexManager::getFamilyDirectory(const void *page, FamilyDirectory &directory) const {
    memcpy(&directory, (uint8_t*) page + sizeof(IndexDirectory), sizeof(directory));
}

NodeType IndexManager::getNodeType(const void *page) const {
    IndexDirectory directory;
    getIndexDirectory(page, directory);
    return directory.type;
}

// compare two keys
// returns negative if key 1 is less than key 2, 0 if they're equal, and positive if key 1 is greater than key 2
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
        // turn varchars into c strings
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
        // use strcmp to do comparison
        return strcmp(key_1_str, key_2_str);
    }
    }
    // should never reach here, but do this so compiler doesn't complain
    return 0;
}

// find the leaf page that a key belongs in
RC IndexManager::findPageWithKey(IXFileHandle &ixfileHandle, const void *key, const Attribute &attribute, void *page, PageNum &pageNum) {
    pageNum = 0;
    if (ixfileHandle.readPage(pageNum, page) != SUCCESS) {
        return IX_READ_FAILED;
    }
    // move down the tree until we reach a leaf node
    while (getNodeType(page) != LEAF_NODE) {
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);
        uint32_t i = 0;
        // find first entry in traffic cops that's greater than the key
        for (; i < node->indexDirectory.numEntries; i++) {
            if (compareAttributeValues(key, node->trafficCops[i], attribute) < 0) {
                // use the page pointer to the left of the found key
                pageNum = node->pagePointers[i];
                break;
            }
        }
        // if we made it all the way to the end of the traffic cops
        if (i == node->indexDirectory.numEntries) {
            // use the rightmost page pointer
            pageNum = node->pagePointers[node->indexDirectory.numEntries];
        }
        if (ixfileHandle.readPage(pageNum, page) != SUCCESS) {
            return IX_READ_FAILED;
        }
        delete node;
    }
    return SUCCESS;
}

// finds the leftmost leaf page
RC IndexManager::findIndexStart(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, PageNum &pageNum) {
    pageNum = 0;
    if (ixfileHandle.readPage(pageNum, page) != SUCCESS) {
        return IX_READ_FAILED;
    }
    while(getNodeType(page) != LEAF_NODE) {
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);
        // always follow the leftmost page pointer
        pageNum = node->pagePointers[0];
        if (ixfileHandle.readPage(pageNum, page) != SUCCESS) {
            return IX_READ_FAILED;
        }
        delete node;
    }
    return SUCCESS;
}

// is there enough room in a leaf node to add a key?
bool IndexManager::canEntryFitInLeafNode(LeafNode &node, const void *key, const Attribute &attribute) {
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

// is there enough room in an interior node to add a key?
bool IndexManager::canEntryFitInInteriorNode(InteriorNode &node, const void *key, const Attribute &attribute) {
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

// add a key / RID pair to a leaf node
RC IndexManager::addEntryToLeafNode(LeafNode &node, const void *key, const RID rid, const Attribute &attribute) {
    // copy the key
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

    // find index to do sorted insert at
    auto cmp_func = [&](const void *a, const void *b){ return compareAttributeValues(a, b, attribute) < 0; };
    int insertion_index = distance(
        node.keys.begin(),
        upper_bound(node.keys.begin(), node.keys.end(), key, cmp_func)
    );

    // add key and RID to node
    node.keys.insert(node.keys.begin() + insertion_index, key_cpy);
    node.rids.insert(node.rids.begin() + insertion_index, rid);
    node.indexDirectory.freeSpaceOffset += key_size + sizeof(RID);
    node.indexDirectory.numEntries++;

    return SUCCESS;
}

// add a new key to an interior node
RC IndexManager::addEntryToInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &node, const void *key, const Attribute &attribute) {
    // copy key
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

    // find index to do sorted insert at
    auto cmp_func = [&](const void *a, const void *b){ return compareAttributeValues(a, b, attribute) < 0; };
    int insertion_index = distance(
        node.trafficCops.begin(),
        upper_bound(node.trafficCops.begin(), node.trafficCops.end(), key, cmp_func)
    );

    // insert new key
    node.trafficCops.insert(node.trafficCops.begin() + insertion_index, key_cpy);
    node.pagePointers.insert(node.pagePointers.begin() + insertion_index + 1, ixfileHandle.getNumberOfPages() - 1);
    node.indexDirectory.freeSpaceOffset += key_size + sizeof(PageNum);
    node.indexDirectory.numEntries++;

    return SUCCESS;
}

RC IndexManager::addEntryToRootNode(IXFileHandle &ixfileHandle, InteriorNode &node, void *key, PageNum leftChild, PageNum rightChild) {
    node.trafficCops.push_back(key);
    node.pagePointers.push_back(leftChild);
    node.pagePointers.push_back(rightChild);
    node.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(node);
    node.indexDirectory.numEntries = node.trafficCops.size();
    return SUCCESS;
}

// insert an entry into the node at the given page number and split if necessary
RC IndexManager::insertAndSplit(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID rid, void *page, PageNum &pageNum){
    void *newKey = malloc(attribute.length);
    if (newKey == NULL)
        return IX_MALLOC_FAILED;

    // Seperate cases for interior node vs leaf node
    if(getNodeType(page) == LEAF_NODE) {
        // load page data into leafNode object
        LeafNode *node = new LeafNode(page, attribute, pageNum);

        // if leaf page is not the root
        // add to vector even if it over fills the FSO
        if(addEntryToLeafNode(*node, key, rid, attribute))
            return LN_ADD_FAILED;
        
        // reference to empty leafNode object that will be split into
        LeafNode *newLeaf = new LeafNode();
        
        if(splitLeafNode(ixfileHandle, *node, *newLeaf, newKey))
            return IM_SPLIT_FAILED;

        // if leaf was a root, we need to create a new root node
        // and add the split nodes as children
        if(pageNum == 0){

            // Setting up the root page.
            void *newPage = malloc(PAGE_SIZE);
            if (newPage == NULL)
                return IX_MALLOC_FAILED;

            newInteriorBasedPage(newPage, -1, -1, 0);

            InteriorNode *newRoot = new InteriorNode(newPage, attribute, 0);
            if(addEntryToRootNode(ixfileHandle, *newRoot, newKey, ixfileHandle.getNumberOfPages()-2, ixfileHandle.getNumberOfPages() - 1))
                return IN_ADD_FAILED;

            newRoot->writeToPage(newPage, attribute); // needs error checking
            ixfileHandle.writePage(0, newPage);

            free(newPage);
            delete node;
            delete newLeaf;
            delete newRoot;

            return SUCCESS;
        // if leaf node was not root, read in its parent and 
        // recursively call function
        }else{
            void *parentPage = malloc(PAGE_SIZE);
            if (parentPage == NULL)
                return IX_MALLOC_FAILED;

            if(ixfileHandle.readPage(node->familyDirectory.parent, parentPage))
                return IX_READ_FAILED;

            RC rc = insertAndSplit(ixfileHandle, attribute, newKey, rid, parentPage, node->familyDirectory.parent); 
            delete newLeaf;
            free(parentPage);
            return rc;
        }
    }else{
        InteriorNode *node = new InteriorNode(page, attribute, pageNum);

        // The new key can fit in the parent node
        if (canEntryFitInInteriorNode(*node, key, attribute)) {
            if(addEntryToInteriorNode(ixfileHandle, *node, key, attribute))
                return IN_ADD_FAILED;
            
            void *newPage = malloc(PAGE_SIZE);
            if (newPage == NULL)
                return IX_MALLOC_FAILED;


            node->writeToPage(newPage, attribute); // needs error checking
            ixfileHandle.writePage(node->selfPageNum, newPage);

            free(newPage);
            delete node;
            return SUCCESS;

        }else{

            if(addEntryToInteriorNode(ixfileHandle, *node, key, attribute))
                return IN_ADD_FAILED;


            // reference to empty leafNode object that will be split into
            InteriorNode *newNode = new InteriorNode();

            if(splitInteriorNode(ixfileHandle, *node, *newNode, newKey))
                return IM_SPLIT_FAILED;

            if(pageNum == 0){
                // Setting up the root page.
                void *newPage = malloc(PAGE_SIZE);
                if (newPage == NULL)
                    return IX_MALLOC_FAILED;

                newInteriorBasedPage(newPage, -1, -1, 0);

                InteriorNode *newRoot = new InteriorNode(newPage, attribute, 0);
                if(addEntryToRootNode(ixfileHandle, *newRoot, newKey, ixfileHandle.getNumberOfPages()-2, ixfileHandle.getNumberOfPages()-1))
                    return IN_ADD_FAILED;

                newRoot->writeToPage(newPage, attribute); // needs error checking
                ixfileHandle.writePage(0, newPage);

                free(newPage);
                delete node;
                delete newRoot;
                delete newNode;

                return SUCCESS;
            // if node was not root, read in its parent and 
            // recursively call function
            }else{
                void *parentPage = malloc(PAGE_SIZE);
                if (parentPage == NULL)
                    return IX_MALLOC_FAILED;

                if(ixfileHandle.readPage(node->familyDirectory.parent, parentPage))
                    return IX_READ_FAILED;

                RC rc = insertAndSplit(ixfileHandle, attribute, newKey, rid, parentPage, node->familyDirectory.parent); 

                free(parentPage);
                delete node;
                delete newNode;

                return rc;
            }
            return SUCCESS;
        }
        


    }
    return SUCCESS;
}

RC IndexManager::splitLeafNode(IXFileHandle &ixfileHandle, LeafNode &originLeaf, LeafNode &newLeaf, void *newKey){
    //set obvious directory values
    newLeaf.indexDirectory.type = LEAF_NODE;
    newLeaf.attribute = originLeaf.attribute;

    if(originLeaf.selfPageNum == 0){
        originLeaf.selfPageNum = ixfileHandle.getNumberOfPages();
        newLeaf.selfPageNum = ixfileHandle.getNumberOfPages() + 1;
    } else {
        // pageNum is new page (need of non heap free page buffer manager)
        newLeaf.selfPageNum = ixfileHandle.getNumberOfPages();
    }
    newLeaf.familyDirectory.leftSibling = originLeaf.selfPageNum;
    newLeaf.familyDirectory.rightSibling = originLeaf.familyDirectory.rightSibling;
    newLeaf.familyDirectory.parent = originLeaf.familyDirectory.parent;

    originLeaf.familyDirectory.rightSibling = newLeaf.selfPageNum;

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
    newLeaf.indexDirectory.numEntries = newLeaf.keys.size();

    // recalculate Free space offset (difficult if varchar)
    originLeaf.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(originLeaf);
    newLeaf.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(newLeaf);

    void *page = malloc(PAGE_SIZE);

    originLeaf.writeToPage(page, originLeaf.attribute);
    if(originLeaf.selfPageNum == ixfileHandle.getNumberOfPages()){
        // write nodes to new page and add to FS
        ixfileHandle.appendPage(page);
    } else {
        // write nodes to new page and add to FS
        ixfileHandle.writePage(originLeaf.selfPageNum, page);
    }

    // write nodes to new page and add to FS
    newLeaf.writeToPage(page, newLeaf.attribute);
    ixfileHandle.appendPage(page);

    // traffic cop is first key of new node (right sibling of origin)
    memcpy(newKey, newLeaf.keys[0], newLeaf.attribute.length);

    return SUCCESS;
}

RC IndexManager::splitInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &originNode, InteriorNode &newNode, void *newKey){
    //set obvious directory values
    newNode.indexDirectory.type = INTERIOR_NODE;
    newNode.attribute = originNode.attribute;

    if(originNode.selfPageNum == 0){
        originNode.selfPageNum = ixfileHandle.getNumberOfPages();
        newNode.selfPageNum = ixfileHandle.getNumberOfPages() + 1;
    } else {
        // pageNum is new page (need of non heap free page buffer manager)
        newNode.selfPageNum = ixfileHandle.getNumberOfPages();
    }

    newNode.familyDirectory.leftSibling = originNode.selfPageNum;
    newNode.familyDirectory.rightSibling = originNode.familyDirectory.rightSibling;
    newNode.familyDirectory.parent = originNode.familyDirectory.parent;

    originNode.familyDirectory.rightSibling = newNode.selfPageNum;

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

    // traffic cop is first key of new node (right sibling of origin)
    memcpy(newKey, newNode.trafficCops[0], newNode.attribute.length);

    newNode.trafficCops.erase(newNode.trafficCops.begin());

    originNode.indexDirectory.numEntries = originNode.trafficCops.size();
    newNode.indexDirectory.numEntries = newNode.trafficCops.size();

    // recalculate Free space offset (difficult if varchar)
    originNode.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(originNode);
    newNode.indexDirectory.freeSpaceOffset = calculateFreeSpaceOffset(newNode);

    void *page = malloc(PAGE_SIZE);

    originNode.writeToPage(page, originNode.attribute);
    if(originNode.selfPageNum == ixfileHandle.getNumberOfPages()){
        // write nodes to new page and add to FS
        ixfileHandle.appendPage(page);
    } else {
        // write nodes to new page and add to FS
        ixfileHandle.writePage(originNode.selfPageNum, page);
    }

    // write nodes to new page and add to FS
    newNode.writeToPage(page, newNode.attribute);
    ixfileHandle.appendPage(page);

    return SUCCESS;
}

uint32_t IndexManager::calculateFreeSpaceOffset(LeafNode &node){
    uint32_t FSO = 0;

    FSO += sizeof(FamilyDirectory);
    FSO += sizeof(IndexDirectory);

    switch (node.attribute.type) {
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

uint32_t IndexManager::calculateFreeSpaceOffset(InteriorNode &node){
    uint32_t FSO = 0;

    FSO += sizeof(FamilyDirectory);
    FSO += sizeof(IndexDirectory);

    switch (node.attribute.type) {
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


/*
 *IXFileHandle Methods
 */

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;

    fileHandle = new FileHandle();

    opened = false;
}

IXFileHandle::~IXFileHandle() {
    fileHandle->~FileHandle();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
    if(fileHandle->readPage(pageNum, data))
        return IX_READ_FAILED;
    ixReadPageCounter++;
    return SUCCESS;
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
    if(fileHandle->writePage(pageNum, data))
        return IX_WRITE_FAILED;
    ixWritePageCounter++;
    return SUCCESS;
}   

RC IXFileHandle::appendPage(const void *data) {
    if(fileHandle->appendPage(data))
        return IX_APPEND_FAILED;
    ixAppendPageCounter++;
    return SUCCESS;
}   

unsigned IXFileHandle::getNumberOfPages() {
    return fileHandle->getNumberOfPages();
}

/*
 * InteriorNode Methods
 */ 

InteriorNode::~InteriorNode() {
    for (void *cop: trafficCops) {
        free(cop);
    }
}

InteriorNode::InteriorNode() {}

// construct an interior node object from a page
InteriorNode::InteriorNode(const void *page, const Attribute &attrib, PageNum pageNum) {
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);
    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    selfPageNum = pageNum;
    attribute = attrib;

    // if there aren't any entries, then don't bother copying traffic cops or page pointers
    if (indexDirectory.numEntries == 0) return;

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

// write the contents of this node object to a page
RC InteriorNode::writeToPage(void *page, const Attribute &attribute) {
    IndexManager *im = IndexManager::instance();

    im->setIndexDirectory(page, indexDirectory);
    im->setFamilyDirectory(page, familyDirectory);

    // pointer to current point in page
    uint8_t *cur_offset = (uint8_t*) page + sizeof(indexDirectory) + sizeof(familyDirectory);
    // add page pointers to page
    for (uint32_t i = 0; i < indexDirectory.numEntries + 1; i++) {
        memcpy(cur_offset, &pagePointers[i], sizeof(PageNum));
        cur_offset += sizeof(PageNum);
    }

    // add traffic cops to page
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

LeafNode::~LeafNode() {
    for (void *key: keys) {
        free(key);
    }
}

LeafNode::LeafNode(){}

// construct a leaf node object from a page
LeafNode::LeafNode(const void *page, const Attribute &attrib, PageNum pageNum){
    IndexManager *im = IndexManager::instance();

    im->getIndexDirectory(page, indexDirectory);

    // set the leafDirectory public variable
    im->getFamilyDirectory(page, familyDirectory);

    selfPageNum = pageNum;
    attribute = attrib;

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

// write this node object back to a page
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

IX_ScanIterator::IX_ScanIterator() {
    curNode = NULL;
}

IX_ScanIterator::~IX_ScanIterator() {

}

// initialize the scanner
RC IX_ScanIterator::init(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive) {
    im = IndexManager::instance();

    this->attribute = attribute;
    this->highKey = highKey;
    this->highKeyInclusive = highKeyInclusive;
    this->ixfileHandle = ixfileHandle;

    void *page = malloc(PAGE_SIZE);
    PageNum pageNum;
    // if there's a low key provided
    if (lowKey != NULL) {
        if (im->findPageWithKey(ixfileHandle, lowKey, attribute, page, pageNum) != SUCCESS) {
            return IX_READ_FAILED;
        }
        curNode = new LeafNode(page, attribute, pageNum);

        // find first key in this page with value >= the low key
        auto pred = [&](void *item) { return im->compareAttributeValues(item, lowKey, attribute) >= 0; };
        cur_index = distance(
            curNode->keys.begin(),
            find_if(curNode->keys.begin(), curNode->keys.end(), pred)
        );
    }
    // else start at the beginning of the index
    else {
        if (im->findIndexStart(ixfileHandle, attribute, page, pageNum) != SUCCESS) {
            return IX_READ_FAILED;
        }
        curNode = new LeafNode(page, attribute, pageNum);
        cur_index = 0;
    }

    // if low key isn't inclusive and the first key selected is equal to the low key, advance
    if (!lowKeyInclusive && im->compareAttributeValues(curNode->keys[cur_index], lowKey, attribute) == 0) {
        advance();
    }

    free(page);
    return SUCCESS;
}

// returns true when this iterator is done scanning
bool IX_ScanIterator::at_end() {
    // if we're all the way at the end of the tree, we're done
    if (cur_index == curNode->keys.size()) {
        return true;
    }

    // if high key is null, then don't check current value against it
    if (highKey == NULL) {
        return false;
    }

    if (highKeyInclusive) {
        return im->compareAttributeValues(curNode->keys[cur_index], highKey, attribute) > 0;
    }
    else {
        return im->compareAttributeValues(curNode->keys[cur_index], highKey, attribute) >= 0;
    }
}

// move to the next entry in the tree, switching pages if necessary
void IX_ScanIterator::advance() {
    if (at_end()) return;

    // if there are still entries in this page, then just advance current index
    if (cur_index < curNode->keys.size() - 1) {
        cur_index++;
    }
    // if there are no more entries in this page, but there is another page to scan, load that page
    else if (curNode->familyDirectory.rightSibling != -1) {
        void *page = malloc(PAGE_SIZE);
        PageNum pageNum = curNode->familyDirectory.rightSibling;
        delete curNode;
        ixfileHandle.readPage(pageNum, page);
        curNode = new LeafNode(page, attribute, pageNum);
        cur_index = 0;
        free(page);
    }
    // else we're done, just increment cur_index so that at_end see that we're done
    else {
        cur_index++;
    }
}
 
RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if (at_end()) {
        return IX_EOF;
    }

    // copy from node into key
    switch(attribute.type) {
    case TypeInt:
    case TypeReal:
        memcpy(key, curNode->keys[cur_index], attribute.length);
        break;
    case TypeVarChar: {
        uint32_t varchar_length;
        memcpy(&varchar_length, curNode->keys[cur_index], VARCHAR_LENGTH_SIZE);
        memcpy(key, curNode->keys[cur_index], VARCHAR_LENGTH_SIZE + varchar_length);
        break;
    }
    }

    rid = curNode->rids[cur_index];

    advance();
    return SUCCESS;
}

RC IX_ScanIterator::close() {
    if (curNode != NULL)
        delete curNode;
    return SUCCESS;
}
