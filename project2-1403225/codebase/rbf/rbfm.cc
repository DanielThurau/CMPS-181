#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RBFM_ScanIterator::RBFM_ScanIterator() {
}

RBFM_ScanIterator::~RBFM_ScanIterator() {
}

RC RBFM_ScanIterator::init(FileHandle fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,
    const void *value,
    const vector<string> &attributeNames) {

    this->fileHandle = fileHandle;
    this->recordDescriptor = recordDescriptor;
    this->conditionAttribute = conditionAttribute;
    this->compOp = compOp;
    this->value = const_cast<void *>(value);
    this->attributeNames = attributeNames;

    // initialize nameToAttribute map
    for (size_t i = 0; i < recordDescriptor.size(); i++){
        nameToAttribute[recordDescriptor[i].name] = recordDescriptor[i];
    }

    // point compFunc to the appropriate comparison function for the type of conditionAttribute
    switch (nameToAttribute[conditionAttribute].type) {
        case TypeInt:
            this->compFunc = checkNumberMeetsCondition<unsigned>;
            break;
        case TypeReal:
            this->compFunc = checkNumberMeetsCondition<float>;
            break;
        case TypeVarChar:
            this->compFunc = checkVarcharMeetsCondition;
            break;
    }

    return SUCCESS;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    // get condition attribute size from nameToAttribute map
    void *attribute = malloc(nameToAttribute[conditionAttribute].length);
    // when the current page num equals the total number of pages, we're out of records
    while (curRid.pageNum < fileHandle.getNumberOfPages()) {
        readAttribute(fileHandle, recordDescriptor, curRid, conditionAttribute, attribute);
        // comp func is set to point to a comparision function appropriate for the type of conditionAttribute
        if (compFunc(attribute, compOp, value)) {
            rid = curRid;
            projectAttributes(data);
        }
        updateCurRid();
    }
    free(attribute);
    return RBFM_EOF;
}

RC RBFM_ScanIterator::updateCurRid() {
    // get current page
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(curRid.pageNum, page);
    SlotDirectoryHeader header = getSlotDirectoryHeader(page);
    // if curRid points to the last record in its page
    if(curRid.slotNum == header.recordEntriesNumber){
        // reset slotNum and advance pageNum
        curRid.slotNum = 0;
        curRid.pageNum++;
    }
    // otherwise just advance slotNum
    else {
        curRid.slotNum++;
    }
    free(page);
    return SUCCESS;
}

// project the attributes in attributeNames from the record pointed to by curRid into data
RC RBFM_ScanIterator::projectAttributes(void *data) {
    // set the null indicator to all 0
    int nullIndicatorSize = getNullIndicatorSize(attributeNames.size());
    memset(data, 0, nullIndicatorSize);

    // curAttribute is a pointer that moves as attributes are added
    char *curAttribute = (char *) data + nullIndicatorSize;
    Attribute attr;
    unsigned varcharLength;

    // for each attribute in attributeNames
    for (size_t i = 0; i < attributeNames.size(); i++) {
        RC rc = readAttribute(fileHandle, recordDescriptor, curRid, attributeNames[i], curAttribute);
        // CHANGE THIS WHEN DAN CHOOSES AN ERROR CODE FOR NULL ATTRIBUTES
        // if retreived attribute is null
        if (rc == -1) {
            // update null indicator
            ((char *) data)[i / 8] |= 128 >> (i % 8);
        }
        else {
            attr = nameToAttribute[attributeNames[i]];
            if (attr.type == TypeInt || attr.type == TypeReal) {
                curAttribute += attr.length;
            }
            else {
                varcharLength = *((unsigned *) curAttribute);
                curAttribute += varcharLength + VARCHAR_LENGTH_SIZE;
            }
        }
    }
    return SUCCESS;
}

template <class T>
bool checkNumberMeetsCondition(void *attribute, CompOp compOp, void *value) {
    if (compOp == NO_OP) return true;

    T attr = *((T*) attribute);
    T val = *((T*) value);

    switch (compOp) {
        case EQ_OP:
            return attr == val;
            break;
        case LT_OP:
            return attr < val;
            break;
        case LE_OP:
            return attr <= val;
            break;
        case GT_OP:
            return attr > val;
            break;
        case GE_OP:
            return attr >= val;
            break;
        case NE_OP:
            return attr != val;
            break;
        default:
            return false;
            break;
    }
}

bool checkVarcharMeetsCondition(void *attribute, CompOp compOp, void *value) {
    if (compOp == NO_OP) return true;

    // get attribute and value lengths from first 4 bytes of varchar
    unsigned attrLength = *((unsigned *) attribute);
    unsigned valueLength = *((unsigned *) value);
    // use min of lengths for memcmp
    unsigned minLength = min<unsigned>(attrLength, valueLength);

    // pointers to start of actual data in varchars
    char *attrStart = (char *) attribute + VARCHAR_LENGTH_SIZE;
    char *valueStart = (char *) value + VARCHAR_LENGTH_SIZE;

    // memcmp
    int diff = memcmp(attrStart, valueStart, minLength);
    // if bytes up to minLength are the same and lengths aren't the same
    if (diff == 0 && attrLength != valueLength) {
        // force the shorter string to be less than the longer string
        diff += attrLength < valueLength ? -1 : 1;
    }

    // save some code by reusing checkNumberMeetsCondition
    // compare result of memcmp to 0
    int zero = 0;
    return checkNumberMeetsCondition<int>(&diff, compOp, &zero);
}

RC RBFM_ScanIterator::close() {
    delete this;

    return 0;
}

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (canRecordFitInPage(pageData, recordSize))
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // check if there are any empty slots in the slot directory
    int emptySlotDirectoryEntryNum = getEmptySlotDirectoryEntry(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    // if there's an empty slot in the slot directory, use that instead of appending a new slot
    rid.slotNum = emptySlotDirectoryEntryNum == -1 ? slotHeader.recordEntriesNumber : emptySlotDirectoryEntryNum;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    if (emptySlotDirectoryEntryNum == -1) {
        slotHeader.recordEntriesNumber += 1;
    }
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // follow all the forwarding addresses to get to the true location of the record
    RID trueRid = followForwardingAddress(fileHandle, rid);

    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(trueRid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < trueRid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, trueRid.slotNum);

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    // delete all the forwarding addresses for this record and get the true rid
    RID trueRid = deleteForwardingAddress(fileHandle, rid);
    
    void * pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(trueRid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // remove the record
    removeRecordFromPage(pageData, trueRid.slotNum);

    // write page back
    if(fileHandle.writePage(trueRid.pageNum, pageData))
        return RBFM_WRITE_FAILED;

    free(pageData);
	return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    RID trueRid = followForwardingAddress(fileHandle, rid);
    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(trueRid.pageNum, pageData);

    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, trueRid.slotNum);

    unsigned newSize = getRecordSize(recordDescriptor, data);

    // how much bigger the new record is than the old one
    // may be negative
    int sizeIncrease = (int) newSize - (int) recordEntry.length;

    // if there's not enough space in this page for the new record, then forward
    if (sizeIncrease > (int) getPageFreeSpaceSize(pageData)) {
        RID newRid;
        insertRecord(fileHandle, recordDescriptor, data, newRid);
        removeRecordFromPage(pageData, trueRid.slotNum);

        // form a forwarding address
        SlotDirectoryRecordEntry forwardingAddress;
        forwardingAddress.length = newRid.slotNum;
        forwardingAddress.offset = -newRid.pageNum; // COME BACK TO THIS.-------------

        // if this record has already been forwarded, then update its original forwarding address
        if (rid.pageNum != trueRid.pageNum) {
            void *origPage = malloc(PAGE_SIZE);
            fileHandle.readPage(rid.pageNum, origPage);
            setSlotDirectoryRecordEntry(origPage, rid.slotNum, forwardingAddress);
            fileHandle.writePage(rid.pageNum, origPage);
            free(origPage);
        }
        // otherwise just add the forwarding address to this page
        else {
            setSlotDirectoryRecordEntry(pageData, trueRid.slotNum, forwardingAddress);
        }
    }
    // if we don't need to forward, then just update this page
    else {
        SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
        void *newDataStart = (char*) pageData + slotHeader.freeSpaceOffset - sizeIncrease;
        void *oldDataStart = (char*) pageData + slotHeader.freeSpaceOffset;
        size_t movedDataLength = recordEntry.offset - slotHeader.freeSpaceOffset;

        // move other records if we need to 
        if (movedDataLength > 0 && newDataStart != oldDataStart) {
            memmove(newDataStart, oldDataStart, movedDataLength);
        }

        // update offsets of all records that were moved
        SlotDirectoryRecordEntry curDirectoryEntry;
        for (int i = 0; i < slotHeader.recordEntriesNumber; i++) {
            curDirectoryEntry = getSlotDirectoryRecordEntry(pageData, i);
            if (curDirectoryEntry.offset < recordEntry.offset && curDirectoryEntry.offset > 0) {
                curDirectoryEntry.offset -= sizeIncrease;
                setSlotDirectoryRecordEntry(pageData, i, curDirectoryEntry);
            }
        }

        // update slot directory entry
        recordEntry.length = newSize;
        recordEntry.offset -= sizeIncrease;
        setSlotDirectoryRecordEntry(pageData, trueRid.slotNum, recordEntry);

        // write new record at its new offset
        setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

        // update free space offset
        slotHeader.freeSpaceOffset -= sizeIncrease;
        setSlotDirectoryHeader(pageData, slotHeader);
    }

    fileHandle.writePage(trueRid.pageNum, pageData);
    free(pageData);

	return SUCCESS;
}

// Return attribute value designated by attributeName
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    // grab index of record being read
    int index = -1;
    for (unsigned i = 0; i < recordDescriptor.size(); i++){
        if (recordDescriptor[i].name == attributeName)
            index = i;
    }
    if(index == -1)
        return RBFM_ATTRIB_DN_EXIST;

    // follow all the forwarding addresses to get to the true location of the record
    RID trueRid = followForwardingAddress(fileHandle, rid);

    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(trueRid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < trueRid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, trueRid.slotNum);

    // Pointer to start of record
    char *start = (char*) pageData + recordEntry.offset;



    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    
    // directory_base: points to the start of our directory of indices
    unsigned data_offset = 0;
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    if (fieldIsNull(nullIndicator, index)){
        return RBFM_NULL;
        // continue;
    }

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    ColumnOffset startPointer;
    ColumnOffset endPointer;
    if(index == 0){
        startPointer = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    }else{

        memcpy(&startPointer, directory_base + (index - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
    }

    memcpy(&endPointer, directory_base + index * sizeof(ColumnOffset), sizeof(ColumnOffset));

    uint32_t fieldSize = endPointer - startPointer;

    if (recordDescriptor[index].type == TypeVarChar)
    {
        memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
        data_offset += VARCHAR_LENGTH_SIZE;
    }
    memcpy((char*) data + data_offset, start + startPointer, fieldSize);




    free(pageData);

	return SUCCESS;
}
/*
// Provided the record descriptor, scan the file.
RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp,
    const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator){

    rbfm_ScanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);

    return SUCCESS;
}
*/
SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// checks to see if a record of length recordLength can fit in a page
// includes check to see if a new slot directory entry needs to be added
bool RecordBasedFileManager::canRecordFitInPage(void * page, unsigned recordLength) {
    unsigned freeSpaceInPage = getPageFreeSpaceSize(page);
    if (getEmptySlotDirectoryEntry(page) == -1) {
        return freeSpaceInPage >= recordLength + sizeof(SlotDirectoryRecordEntry);
    }
    else {
        return freeSpaceInPage >= recordLength;
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

int RecordBasedFileManager::getEmptySlotDirectoryEntry(void *page) {
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    SlotDirectoryRecordEntry entry;

    for (int i = 0; i < slotHeader.recordEntriesNumber; i++) {
        entry = getSlotDirectoryRecordEntry(page, i);
        if (entry.length == 0 && entry.offset == 0) {
            return i;
        }
    }

    return -1;
}

// follows all forwarding addresses to find true rid for a record
RID RecordBasedFileManager::followForwardingAddress(FileHandle fileHandle, RID startRid) {
    void * pageData = malloc(PAGE_SIZE);
    RID curRid = startRid;

    fileHandle.readPage(curRid.pageNum, pageData);
    SlotDirectoryRecordEntry entry = getSlotDirectoryRecordEntry(pageData, curRid.slotNum);
    if (entry.offset < 0) {
        curRid.pageNum = -entry.offset;
        curRid.slotNum = entry.length;
    }

    free(pageData);
    return curRid;
}

// deletes the forwarding address associated with a record and returns the true rid
RID RecordBasedFileManager::deleteForwardingAddress(FileHandle fileHandle, RID startRid) {
    void * pageData = malloc(PAGE_SIZE);
    RID curRid = startRid;

    fileHandle.readPage(curRid.pageNum, pageData);
    SlotDirectoryRecordEntry entry = getSlotDirectoryRecordEntry(pageData, curRid.slotNum);
    if (entry.offset < 0) {
        deleteSlotDirectoryEntry(pageData, curRid.slotNum);
        fileHandle.writePage(curRid.pageNum, pageData);

        curRid.pageNum = -entry.offset;
        curRid.slotNum = entry.length;
    }

    free(pageData);
    return curRid;
}

// delete an entry from a slot directory
void RecordBasedFileManager::deleteSlotDirectoryEntry(void *page, unsigned slotNum) {
    SlotDirectoryRecordEntry emptyDirectoryEntry;
    emptyDirectoryEntry.length = 0;
    emptyDirectoryEntry.offset = 0;

    setSlotDirectoryRecordEntry(page, slotNum, emptyDirectoryEntry);
}

void RecordBasedFileManager::removeRecordFromPage(void *page, unsigned slotNum) {
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    
    // get the record entry for this record and then delete the record entry from this page
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, slotNum);
    deleteSlotDirectoryEntry(page, slotNum);

    // move records to coalesce free space in center of page
    void *newDataStart = (char*) page + slotHeader.freeSpaceOffset + recordEntry.length;
    void *oldDataStart = (char*) page + slotHeader.freeSpaceOffset;
    size_t movedDataLength = recordEntry.offset - slotHeader.freeSpaceOffset;
    // if there are any records to move
    if (movedDataLength > 0) {
        memmove(newDataStart, oldDataStart, movedDataLength);
    }

    // update offsets of all records that were moved
    SlotDirectoryRecordEntry curDirectoryEntry;
    for (int i = 0; i < slotHeader.recordEntriesNumber; i++) {
        curDirectoryEntry = getSlotDirectoryRecordEntry(page, i);
        if (curDirectoryEntry.offset < recordEntry.offset && curDirectoryEntry.offset > 0) {
            curDirectoryEntry.offset += recordEntry.length;
            setSlotDirectoryRecordEntry(page, i, curDirectoryEntry);
        }
    }

    // update free space offset
    slotHeader.freeSpaceOffset += recordEntry.length;
    setSlotDirectoryHeader(page, slotHeader);
}

unsigned RecordBasedFileManager::attributeOffsetFromIndex(void *record, unsigned index, const vector<Attribute> &recordDescriptor){
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, record, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    for (unsigned i = 0; i < index; i++){
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                offset += INT_SIZE;
            break;
            case TypeReal:
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
               // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) record + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;
                offset += varcharSize;
            break;
        }
    }
    return offset;
}