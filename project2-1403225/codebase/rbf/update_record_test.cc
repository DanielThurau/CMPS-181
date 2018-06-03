#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"
#include <string.h>

int test_delete_record(RecordBasedFileManager *rbfm) {
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    string fileName = "test_ur";

    RC rc;
    rc = rbfm->createFile(fileName);
	rc = createFileShouldSucceed(fileName);
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);

    RID rid_1;
    RID rid_2;
    void *record_1 = malloc(100);
    void *record_2 = malloc(100);
    int record_1_size;
    int record_2_size;

    void *returned_data = malloc(100);
    
    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    int numNullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char nullFieldIndicator[numNullBytes];
    memset(nullFieldIndicator, 0, numNullBytes);

    prepareRecord(recordDescriptor.size(), nullFieldIndicator, 3, "Cow", 21, 5.917, 150000, record_1, &record_1_size);
    prepareRecord(recordDescriptor.size(), nullFieldIndicator, 20, "AAAAABBBBBCCCCCDDDDD", 21, 6.25, 20000, record_2, &record_2_size);

    do {
        rbfm->insertRecord(fileHandle, recordDescriptor, record_1, rid_1);
    }
    while (rid_1.pageNum == 0);

    rid_2.pageNum = 0;
    rid_2.slotNum = 0;

    rbfm->updateRecord(fileHandle, recordDescriptor, record_2, rid_2);
    rbfm->readRecord(fileHandle, recordDescriptor, rid_2, returned_data);

    if (memcmp(record_2, returned_data, record_2_size) != 0) {
        printf("SUCK\n");
    }

    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(0, page);
    for (int i = 0; i < PAGE_SIZE; i++) {
        printf("%u ", ((uint8_t *) page)[i]);
    }
    printf("\n\n\n\n\n");
    fileHandle.readPage(1, page);
    for (int i = 0; i < PAGE_SIZE; i++) {
        printf("%u ", ((uint8_t *) page)[i]);
    }
    printf("\n");

    free(record_1);
    free(record_2);

    return rc;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test_ur");
       
    RC rcmain = test_delete_record(rbfm);
    return rcmain;
}
