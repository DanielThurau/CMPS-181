
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cmath>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

#define TABLE_FILE_EXTENSION ".t"
#define INDEX_FILE_EXTENSION ".ix"

#define TABLES_TABLE_NAME           "Tables"
#define TABLES_TABLE_ID             1

// Format for Tables table:
// (table-id:int, table-name:varchar(50), file-name:varchar(50), system:int)
// system will be 1 if the table is a system table, 0 otherwise

#define TABLES_COL_TABLE_ID         "table-id"
#define TABLES_COL_TABLE_NAME       "table-name"
#define TABLES_COL_FILE_NAME        "file-name"
#define TABLES_COL_SYSTEM           "system"
#define TABLES_COL_TABLE_NAME_SIZE  50
#define TABLES_COL_FILE_NAME_SIZE   50

// 1 null byte, 2 integer and 2 varchars
#define TABLES_RECORD_DATA_SIZE 1 + 4 * INT_SIZE + TABLES_COL_TABLE_NAME_SIZE + TABLES_COL_FILE_NAME_SIZE

#define COLUMNS_TABLE_NAME           "Columns"
#define COLUMNS_TABLE_ID             2

#define COLUMNS_COL_TABLE_ID         "table-id"
#define COLUMNS_COL_COLUMN_NAME      "column-name"
#define COLUMNS_COL_COLUMN_TYPE      "column-type"
#define COLUMNS_COL_COLUMN_LENGTH    "column-length"
#define COLUMNS_COL_COLUMN_POSITION  "column-position"
#define COLUMNS_COL_COLUMN_NAME_SIZE 50

// 1 null byte, 4 integer fields and a varchar
#define COLUMNS_RECORD_DATA_SIZE 1 + 5 * INT_SIZE + COLUMNS_COL_COLUMN_NAME_SIZE

#define INDEXES_TABLE_NAME           "Indexes"
#define INDEXES_TABLE_ID             3

#define INDEXES_COL_TABLE_NAME       "table-name"
#define INDEXES_COL_ATTR_NAME        "attr-name"
#define INDEXES_COL_INDEX_FILENAME   "index-filename"
#define INDEXES_COL_TABLE_NAME_SIZE  50
#define INDEXES_COL_ATTR_NAME_SIZE   50
#define INDEXES_COL_INDEX_FILENAME_SIZE 50

// 3 varchars
#define INDEXES_RECORD_DATA_SIZE 1 + INDEXES_COL_TABLE_NAME_SIZE + INDEXES_COL_ATTR_NAME_SIZE + INDEXES_COL_INDEX_FILENAME_SIZE

# define RM_EOF (-1)  // end of a scan operator

#define RM_CANNOT_MOD_SYS_TBL 1
#define RM_NULL_COLUMN        2
#define RM_NO_MATCHING_INDEX  3
#define RM_ATTR_NOT_FOUND     4

typedef struct IndexedAttr
{
    int32_t pos;
    Attribute attr;
} IndexedAttr;

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

  friend class RelationManager;
private:
  RBFM_ScanIterator rbfm_iter;
  FileHandle fileHandle;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(); 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan

  friend class RelationManager;
 private:
  IX_ScanIterator ix_scanIterator;
  IXFileHandle ixfileHandle;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  const vector<Attribute> tableDescriptor;
  const vector<Attribute> columnDescriptor;
  const vector<Attribute> indexDescriptor;

  // Convert tableName to file name (append extension)
  static string getFileName(const char *tableName);
  static string getFileName(const string &tableName);

  // Create recordDescriptor for Table/Column tables
  static vector<Attribute> createTableDescriptor();
  static vector<Attribute> createColumnDescriptor();
  static vector<Attribute> createIndexDescriptor();

  // Prepare an entry for the Table/Column table
  void prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data);
  void prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data);
  void prepareIndexRecordData(const string &table_name, const string &attr_name, const string &index_filename, void *data);

  // Given a table ID and recordDescriptor, creates entries in Column table
  RC insertColumns(int32_t id, const vector<Attribute> &recordDescriptor);
  // Given table ID, system flag, and table name, creates entry in Table table
  RC insertTable(int32_t id, int32_t system, const string &tableName);

  // Get next table ID for creating table
  RC getNextTableID(int32_t &table_id);
  // Get table ID of table with name tableName
  RC getTableID(const string &tableName, int32_t &tableID);

  RC isSystemTable(bool &system, const string &tableName);

  // get filename and rid for an index given table name and attribute name
  RC getIndexFilename(const string &tableName, const string &attributeName, string &fileName, RID &rid);
  // update all indexes for the given table
  RC updateIndexes(const string &tableName, const void *data, const RID &rid);
  // get the index'th attribute from a tuple
  RC getAttrFromTuple(const vector<Attribute> attrs, int index, const void *tuple, void *key);

  // Utility functions for converting single values to/from api format
  // Useful when using ScanIterators
  void fromAPI(float &real, void *data);
  void fromAPI(string &str, void *data);
  void fromAPI(int32_t &integer, void *data);
  void toAPI(const float real, void *data);
  void toAPI(const int32_t integer, void *data);
  void toAPI(const string &str, void *data);

};

#endif
