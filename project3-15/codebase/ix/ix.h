#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <iostream>
#include <algorithm>

#include "../rbf/rbfm.h"

#define IX_EOF (-1)  // end of the index scan

#define SUCCESS 0

#define IX_CREATE_FAILED     1
#define IX_MALLOC_FAILED     2
#define IX_OPEN_FAILED       3
#define IX_APPEND_FAILED     4
#define IX_READ_FAILED       5
#define IX_WRITE_FAILED      6 
#define IX_FILE_NOT_OPENED   7

#define IM_KEY_NOT_FOUND     1
#define IM_ADD_ENTRY_FAILED  2
#define IM_SPLIT_FAILED      3

#define LN_WRITE_FAILED      1
#define LN_ADD_FAILED        2

#define IN_WRITE_FAILED      1
#define IN_ADD_FAILED        2

typedef enum {
    INTERIOR_NODE = 0,
    LEAF_NODE
} NodeType;


typedef struct IndexDirectory {
    uint32_t numEntries;
    uint32_t freeSpaceOffset;
    NodeType type;
} IndexDirectory;

// Struct for leaf page sibling references
// -1 indicated null reference if furthest 
// left/right sibling
typedef struct FamilyDirectory {
    PageNum parent;
    int32_t leftSibling;
    int32_t rightSibling;
} FamilyDirectory;

class InteriorNode {
public:
    // destructor
    ~InteriorNode();
    InteriorNode();
    InteriorNode(const void *page, const Attribute &attrib, PageNum pageNum);
    RC writeToPage(void *page, const Attribute &attribute);

    PageNum selfPageNum;
    Attribute attribute;

    IndexDirectory  indexDirectory;
    FamilyDirectory familyDirectory;

    vector<void *> trafficCops;
    vector<PageNum> pagePointers;
};

class LeafNode {
public:
    // destructor
    ~LeafNode();
    // page not created
    LeafNode();
    // load page data into class
    LeafNode(const void *page, const Attribute &attrib, PageNum pageNum);
    RC writeToPage(void *page, const Attribute &attribute);

    PageNum selfPageNum;
    Attribute attribute;

    IndexDirectory  indexDirectory;
    FamilyDirectory familyDirectory;

    vector<void*> keys;
    vector<RID> rids;
};

class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // true if this file handle has been attached to a file
        bool opened;

        FileHandle* fileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

    	// Put the current counter values of associated PF FileHandles into variables
    	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();

};

class IndexManager;

class IX_ScanIterator {
    private:
        IXFileHandle ixfileHandle;
        Attribute attribute;
        const void *highKey;
        bool highKeyInclusive;

        LeafNode *curNode;
        IndexManager *im;
        size_t cur_index;        
        
        bool at_end();
        void advance();

    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        RC init(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive);

        // Terminate index scan
        RC close();
};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        friend class InteriorNode;
        friend class LeafNode;
        friend class IX_ScanIterator;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        static PagedFileManager *_pf_manager;


        void newLeafBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent);
        void newInteriorBasedPage(void *page, int32_t leftSibling, int32_t rightSibling, PageNum parent);

        void getIndexDirectory(const void *page, IndexDirectory &directory) const;
        void setIndexDirectory(void *page, IndexDirectory &directory);

        void getFamilyDirectory(const void *page, FamilyDirectory &directory) const;
        void setFamilyDirectory(void *page, FamilyDirectory &directory);

        NodeType getNodeType(const void *page) const;
        int compareAttributeValues(const void *key_1, const void *key_2, const Attribute &attribute) const;
        RC findPageWithKey(IXFileHandle &ixfileHandle, const void *key, const  Attribute &attribute, void *page, PageNum &pageNum);
        RC findIndexStart(IXFileHandle &ixfileHandle, const Attribute &attribute, void *page, PageNum &pageNum);

        bool canEntryFitInLeafNode(LeafNode &node, const void *key, const Attribute &attribute);
        bool canEntryFitInInteriorNode(InteriorNode &node, const void *key, const Attribute &attribute);
        RC addEntryToLeafNode(LeafNode &node, const void *key, RID rid, const Attribute &attribute);
        RC addEntryToInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &node, const void *key, const Attribute &attribute);
        RC addEntryToRootNode(IXFileHandle &ixfileHandle, InteriorNode &node, void *key, PageNum leftChild, PageNum rightChild);

        void printTreeRecur(IXFileHandle &ixfileHandle, const Attribute &attribute, PageNum pageNum, int depth) const;
        void printInteriorNode(IXFileHandle &ixfileHandle, const Attribute &attribute, InteriorNode &node, int depth) const;
        void printLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute, LeafNode &node, int depth) const;
        void printKey(void *key, const Attribute &attribute) const;

        RC insertAndSplit(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID rid, void *page, PageNum &pageNum);
        RC splitLeafNode(IXFileHandle &ixfileHandle, LeafNode &originLeaf, LeafNode &newLeaf, void *newKey);
        RC splitInteriorNode(IXFileHandle &ixfileHandle, InteriorNode &originNode, InteriorNode &newNode, void *newKey);
        uint32_t calculateFreeSpaceOffset(LeafNode &node);
        uint32_t calculateFreeSpaceOffset(InteriorNode &node);
};


#endif
