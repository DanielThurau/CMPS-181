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
    unsigned recordLength = 0;
    for (size_t i = 0; i < recordDescriptor.size(); i++) {
        // art
        bool attrNull = (128 >> (i % 8)) & *((uint8_t*)data + i / 8);
        if (!attrNull) {
            recordLength += recordDescriptor[i].length;
        }
    }

    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
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
            printf("NULL    ");
            continue;
        }

        if (recordDescriptor[i].type == TypeInt) {
            printf("%-8d", *((int*)curField));
            curField += recordDescriptor[i].length;
        }
        else if (recordDescriptor[i].type == TypeReal) {
            printf("%-8f", *((float*)curField));
            curField += recordDescriptor[i].length;
        }
        else {
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
