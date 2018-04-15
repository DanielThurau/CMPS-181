#include <math.h>
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
    printf("%u\n", recordLength);

    void *data2 = malloc(PAGE_SIZE);
    createNewPage(data2);
    for (int i = 0; i < PAGE_SIZE / 4; i++) {
        printf("%u ", *((unsigned*) data2 + i));
    }
    printf("\n");

    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

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
    recordLength = (unsigned) (curField - (uint8_t*)data);
    return 0;
}

RC RecordBasedFileManager::createNewPage(void *data) {
    // write 0 to last 4 bytes of data for offset to start of free space
    *((unsigned*) data + (PAGE_SIZE / 4) - 1) = 0;
    // write 0 to second-to-last 4 bytes of data for num of slots
    *((unsigned*) data + (PAGE_SIZE / 4) - 2) = 0;

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
            printf("%d", *((int*)curField));
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeReal) {
            printf("%f", *((float*)curField));
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeVarChar) {
            // first 4 bytes of varchar is length 
            unsigned stringLength = *((unsigned*) curField);
            curField += 4;
            // print each char in string one by one
            for (unsigned j = 0; j < stringLength; j++) {
                putchar(*(curField + j));
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
