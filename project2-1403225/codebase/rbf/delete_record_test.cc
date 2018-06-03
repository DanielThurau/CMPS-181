#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

int test_delete_record(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 8 *****" << endl;
   
    RC rc;
    string fileName = "test_dr";

    rc = rbfm->createFile(fileName);
	rc = createFileShouldSucceed(fileName);
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);

    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 24, 170.1, 5000, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");

    RID newrid;
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, newrid);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }

    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    assert(rc == success && "Deleting a record should not fail.");
    
    rc = rbfm->readRecord(fileHandle, recordDescriptor, newrid, returnedData);
    assert(rc == success && "Reading a record should not fail.");
    
    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }

    char *pageData = (char*) malloc(PAGE_SIZE);
    fileHandle.readPage(0, pageData);

    cout << endl;

    rc = rbfm->closeFile(fileHandle);
    rc = rbfm->destroyFile(fileName);

    free(record);
    free(returnedData);
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test_dr");
       
    RC rcmain = test_delete_record(rbfm);
    return rcmain;
}
