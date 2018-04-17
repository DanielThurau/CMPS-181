#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    this->pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    RC rc;
    if((rc = this->pfm->createFile(fileName)) != 0){
        eprintf("PagedFileManager API call: createFile failed on file %s", fileName.c_str());
        return rc;
    }

    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    RC rc;
    if((rc = this->pfm->destroyFile(fileName)) != 0){
        eprintf("PagedFileManager API call: destroyFile failed on file %s", fileName.c_str());
        return rc;
    }

    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    RC rc;
    if((rc = this->pfm->openFile(fileName, fileHandle)) != 0){
        eprintf("PagedFileManager API call: openFile failed on file %s", fileName.c_str());
        return rc;
    }

    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    RC rc;
    if((rc = this->pfm->closeFile(fileHandle)) != 0){
        eprintf("PagedFileManager API call: closeFile failed");
        return rc;
    }

    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    

    unsigned recordLength;
    getRecordLength(recordDescriptor, data, recordLength);

    void* newData = malloc(recordLength + recordDescriptor.size() * 2);
    addDirectoryToRecord(recordDescriptor, data, newData);

    for (unsigned i = 0; i < recordLength + recordDescriptor.size() * 2; i++) {
        printf("%u ", ((uint8_t*)newData)[i]);
    }
    printf("\n");

/*
    unsigned pageNum;
    getNextPage(fileHandle, recordLength, pageNum);

    unsigned sid;
    writeRecord(fileHandle, recordLength, pageNum, data, sid);

    rid.pageNum = pageNum;
    rid.slotNum = sid;
 */
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);
    unsigned *pageUnsigned = (unsigned*) page;

    unsigned recordSize = pageUnsigned[PAGE_SIZE / 4 - 2 - (rid.slotNum + 1) * 2 + 1];
    unsigned offset = pageUnsigned[PAGE_SIZE / 4 - 2 - (rid.slotNum + 1) * 2];

    memcpy(data, (uint8_t*) page + offset, recordSize);

    free(page);
    return 0;
}

RC RecordBasedFileManager::addDirectoryToRecord(const vector<Attribute> &recordDescriptor, const void *dataIn, void *dataOut) {
    int numNullBytes = ceil(recordDescriptor.size() / 8.0);
    memcpy(dataOut, dataIn, numNullBytes);
    // pointer to start of field pointer directory in reformatted record
    unsigned short *curFieldPointer = (unsigned short *)((uint8_t*) dataOut + numNullBytes);

    // pointer to current field in old record
    uint8_t *curField = (uint8_t*) dataIn + numNullBytes;
    // pointer to start of fields in old record
    uint8_t *startOfFields = curField;

    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        // if null flag is set for this field, continue
        if ((128 >> (i % 8)) & *((uint8_t*) dataIn + i / 8)) {
            *curFieldPointer = (unsigned short)(curField - startOfFields);
            curFieldPointer++;
            continue;
        }

        if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
            curField += recordDescriptor[i].length;
            *curFieldPointer = (unsigned short)(curField - startOfFields);
            curFieldPointer++;
        }
        else if (recordDescriptor[i].type == TypeVarChar) {
            // first 4 bytes of varchar is length 
            unsigned stringLength = *((unsigned*) curField);
            curField += stringLength + 4;
            *curFieldPointer = (unsigned short)(curField - startOfFields);
            curFieldPointer++;
        }
    }

    int fieldsSize = curField - startOfFields;
    memcpy(curFieldPointer, startOfFields, fieldsSize);
    return 0;
}

// gets the length of a record
RC RecordBasedFileManager::getRecordLength(const vector<Attribute> & recordDescriptor, const void *data, unsigned &recordLength) {
    // number of bytes for null flag
    int numNullBytes = ceil(recordDescriptor.size() / 8.0);
    // pointer to current field
    uint8_t *curField = (uint8_t*) data + numNullBytes;

    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        // if null flag is set for this field, continue
        if ((128 >> (i % 8)) & *((uint8_t*) data + i / 8)) {
            continue;
        }

        if (recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeVarChar) {
            // first 4 bytes of varchar is length 
            unsigned stringLength = *((unsigned*) curField);
            curField += stringLength + 4;
        }
    }
    recordLength = (unsigned) (curField - (uint8_t*) data);
    return 0;
}

// initializes a page to be used to store records
RC RecordBasedFileManager::createNewPage(void *data) {
    unsigned *pageUnsigned = (unsigned*) data;

    // write 0 to last 4 bytes of data for offset to start of free space
    pageUnsigned[PAGE_SIZE / 4 - 1] = 0;
    // write 0 to second-to-last 4 bytes of data for num of slots
    pageUnsigned[PAGE_SIZE / 4 - 2] = 0;

    return 0;
}

// gets the amount of space left in a page
RC RecordBasedFileManager::getAvailableSpaceInPage(const void *data, unsigned &space) {
    unsigned *pageUnsigned = (unsigned*) data;

    // get offset to start of free space from last 4 bytes of page
    unsigned freeSpaceOffset = pageUnsigned[PAGE_SIZE / 4 - 1];
    // get number of records from second-to-last 4 bytes of page
    unsigned numRecords = pageUnsigned[PAGE_SIZE / 4 - 2];

    // remaining free space in page is page size - space occupied by records - 8 bytes for offset and num records
    // - 8 bytes per record for record directory
    space = PAGE_SIZE - freeSpaceOffset - 8 - numRecords * 8;
    return 0;
}

// writes a record into a page
// given the page number, the length of the record and a pointer to the record
// returns the slot id the record is given
RC RecordBasedFileManager::writeRecord(FileHandle &fileHandle, unsigned recordLength, unsigned pageNum, const void *data, unsigned &sid) {
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, page);
    unsigned *pageUnsigned = (unsigned*) page;

    // get offset to free space and number of records from end of page
    unsigned freeSpaceOffset = pageUnsigned[PAGE_SIZE / 4 - 1];
    unsigned numRecords = pageUnsigned[PAGE_SIZE / 4 - 2];

    // copy record into page starting at free space offset
    memcpy((uint8_t*)page + freeSpaceOffset, data, recordLength);

    // add new record offset and length to record directory
    pageUnsigned[PAGE_SIZE / 4 - 2 - numRecords * 2 - 2] = freeSpaceOffset;
    pageUnsigned[PAGE_SIZE / 4 - 2 - numRecords * 2 - 1] = recordLength;

    // return slot id
    sid = numRecords;

    // update number of records and offset to free space in page
    pageUnsigned[PAGE_SIZE / 4 - 2] = numRecords + 1;
    pageUnsigned[PAGE_SIZE / 4 - 1] = freeSpaceOffset + recordLength;

    fileHandle.writePage(pageNum, page);

    free(page);
    return 0;
}

// gets the next page with enough space available to store a record of length recordLength
RC RecordBasedFileManager::getNextPage(FileHandle &fileHandle, unsigned recordLength, unsigned &pageNum) {
    unsigned numPages = fileHandle.getNumberOfPages();
    void* data = malloc(PAGE_SIZE);
    unsigned space;

    // check the last page
    if (numPages > 0) {
        fileHandle.readPage(numPages - 1, data);
        getAvailableSpaceInPage(data, space);
        if(space >= recordLength + 8){
            pageNum = numPages - 1;
            free(data);
            return 0;
        }
    }

    // if the last page doesn't have space, check all the other pages
    for(unsigned pg_offset = 0; pg_offset < numPages - 1; pg_offset++){
        fileHandle.readPage(pg_offset, data);
        getAvailableSpaceInPage(data, space);
        if(space >= recordLength + 8){
            pageNum = pg_offset;
            free(data);
            return 0;
        }
    }

    // if no pages have space, append a new page
    createNewPage(data);
    fileHandle.appendPage(data);
    pageNum = ++numPages;
    free(data);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // number of bytes for null flag
    int numNullBytes = ceil(recordDescriptor.size() / 8.0);
    // pointer to current field
    uint8_t *curField = (uint8_t*) data + numNullBytes;

    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        printf("%s: ", recordDescriptor[i].name.c_str());
        // if null flag is set for this field, print NULL and continue
        if ((128 >> (i % 8)) & *((uint8_t*) data + i / 8)) {
            printf("NULL");
            continue;
        }

        if (recordDescriptor[i].type == TypeInt) {
            printf("%d", *((int*) curField));
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeReal) {
            printf("%f", *((float*) curField));
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeVarChar) {
            // first 4 bytes of varchar is length 
            unsigned stringLength = *((unsigned*) curField);
            curField += 4;
            // print each char in string one by one
            for (unsigned j = 0; j < stringLength; j++) {
                putchar(curField[j]);
            }
            curField += stringLength;
        }

        // add a space between fields
        if (i < recordDescriptor.size() - 1) {
            printf(" ");
        }

    }
    printf("\n");
    return 0;
}
