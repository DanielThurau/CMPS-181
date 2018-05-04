
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <inttypes.h>
#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define RM_TABLE_NOT_FOUND 1




// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  FileHandle fileHandle;
  RBFM_ScanIterator scanner;
  
  RM_ScanIterator();
  ~RM_ScanIterator();

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
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


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  string tablesFileName;
  string columnsFileName;
  int tableCounter;


  void createTableDescriptor(vector<Attribute> &tableDescriptor);
  void createColumnDescriptor(vector<Attribute> &columnDescriptor);
  
  void prepareTables(int table_id, const string &table_name, const string &file_name, void *buffer, vector<Attribute> &tableDescriptor);
  void prepareColumns(int table_id, const string &column_name, int column_type, int column_length, int column_position, void *buffer, vector<Attribute> &tableDescriptor);

  RC createCatalogColumns();
  RC createCatalogTables();
  
  vector<Attribute> assembleAttributes(unsigned tableID);
  int columnEntry(void *columnRecord, Attribute &entry, vector<string> projectionAttributes);
  
  int getActualByteForNullsIndicator(int fieldCount);
  RC getTableIDAndFilename (const string tableName, string &filename, unsigned &tableId);
};

#endif
