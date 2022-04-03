#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */

/* partition(): Given input disk, memory, and the disk page ID ranges for the left and right relations
(represented as an std::pair <begin, end> for a relation, its data is stored in disk page [begin, end), where ‘end’ is excluded),
perform the data record partition. The output is a vector of buckets of size (MEM_SIZE_IN_PAGE - 1),
in which each bucket stores all the disk page IDs and number of records for the left and right relations in one specific partition.*/

void partitionRelation(unsigned int begin, unsigned int end, Disk *disk, Mem *mem,
                       unsigned int inputPageIndex, unsigned int bufferPages, vector<Bucket> &buckets, int rightOrLeft);

void probeRelation(vector<unsigned int> &diskPageIDs, vector<unsigned int> &smallRelationPageIndices, vector<unsigned int> &largeRelationPageIndices,
Disk *disk, Mem *mem, unsigned int bufferPages, unsigned int inputPageIndex, unsigned int outputPageIndex, bool finalProbe);

vector<Bucket> partition(
    Disk *disk,
    Mem *mem,
    pair<unsigned int, unsigned int> left_rel,
    pair<unsigned int, unsigned int> right_rel)
{

    // 1 page for input
    // B-1 pages for partions

    unsigned int inputPageIndex = 0;
    unsigned int bufferPages = MEM_SIZE_IN_PAGE - 1;

    // the amount of buckets is proprotional to the amount of buffer pages
    vector<Bucket> buckets(bufferPages, Bucket(disk));

    partitionRelation(left_rel.first, left_rel.second, disk, mem, inputPageIndex, bufferPages, buckets, 1);
    partitionRelation(right_rel.first, right_rel.second, disk, mem, inputPageIndex, bufferPages, buckets, 0);

    // for(unsigned int i = 0; i < buckets.size(); ++i) {

    //     vector<unsigned int> leftDiskPageIds = buckets[i].get_left_rel();
    //     vector<unsigned int> rightDiskPages = buckets[i].get_right_rel();
    // for(int x = 0; x < leftDiskPageIds.size(); ++x) {
    //     cout << "Bucket " << i << " " << leftDiskPageIds[x] << "\n";
    // }

    // vector<unsigned int> rightDiskPageIds = buckets[i].get_right_rel();
    //     cout << "partition: " << i << "\n";
    //     cout << "left relation records:\n";
    //     for(vector<unsigned int>::iterator it = leftDiskPageIds.begin(); it != leftDiskPageIds.end(); ++it) {
    //         mem->loadFromDisk(disk, *it, 0);
    //         Page *p = mem->mem_page(0);
    //         for(unsigned int i = 0; i < p->size(); ++i) {
    //             Record r = p->get_record(i);
    //             r.print();
    //         }
    //     }
    //     cout << "right relation records:\n";
    //     for(vector<unsigned int>::iterator it = rightDiskPages.begin(); it != rightDiskPages.end(); ++it) {
    //         mem->loadFromDisk(disk, *it, 0);
    //         Page *p = mem->mem_page(0);
    //         for(int i = 0; i < p->size(); ++i) {
    //             Record r = p->get_record(i);
    //             r.print();
    //         }
    //     }

    //     cout <<"\n\n";

    // }

    return buckets;
}

void partitionRelation(unsigned int begin, unsigned int end, Disk *disk, Mem *mem,
                       unsigned int inputPageIndex, unsigned int bufferPages, vector<Bucket> &buckets, int rightOrLeft)
{

    // partion left relation
    // we want to iterate over every disk page in the relation
    for (unsigned int diskPageIndex = begin; diskPageIndex < end; ++diskPageIndex)
    {

        // load the page from disk to inputpage
        mem->loadFromDisk(disk, diskPageIndex, inputPageIndex);
        Page *inputPage = mem->mem_page(inputPageIndex);

        // hash each record in this page
        // data record returned from get_record will be in range of [0, size)
        for (unsigned int recordIndex = 0; recordIndex < inputPage->size(); ++recordIndex)
        {
            // cout << "HERE" << "\n";

            Record record = inputPage->get_record(recordIndex);
            // record.print();
            unsigned int hashValPageIndex = (record.partition_hash() % bufferPages) + 1; // add 1 since page 0 reserved for input
            // cout << hashVal << "\n\n";

            // now load this record into buffer page from the hashVal
            Page *bufferPage = mem->mem_page(hashValPageIndex);
            bufferPage->loadRecord(record);

            // check if we are in last itteration, and flush all buffer pages to disk if it is
            if (diskPageIndex == end - 1)
            {

                // loop over every buffer page
                // bufferPageIndex starts = 1 since 0 is reserved for inputPage
                for (unsigned int bufferPageIndex = 1; bufferPageIndex <= bufferPages; ++bufferPageIndex)
                {

                    unsigned int diskpage = mem->flushToDisk(disk, bufferPageIndex);

                    if (rightOrLeft == 0)
                    {
                        buckets[bufferPageIndex - 1].add_right_rel_page(diskpage); //-1 since the buckets array is 0 indexed
                    }
                    else if (rightOrLeft == 1)
                    {
                        buckets[bufferPageIndex - 1].add_left_rel_page(diskpage);
                    }

                    // clear the page since we flushed to disk
                    Page *p = mem->mem_page(bufferPageIndex);
                    p->reset();
                }
                continue;
            }

            // if this page is full flush to disk
            if (bufferPage->size() == RECORDS_PER_PAGE)
            {
                unsigned int diskpage = mem->flushToDisk(disk, hashValPageIndex);

                if (rightOrLeft == 0)
                {
                    buckets[hashValPageIndex - 1].add_right_rel_page(diskpage);
                }
                else if (rightOrLeft == 1)
                {
                    buckets[hashValPageIndex - 1].add_left_rel_page(diskpage);
                }

                // clear the page since we flushed it
                bufferPage->reset();
            }
        }
    }
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
/* probe(): Given disk, memory, and a vector of buckets, perform the probing.
The output is a vector of integers, which stores all the disk page IDs of the join result.*/
vector<unsigned int> probe(Disk *disk, Mem *mem, vector<Bucket> &partitions)
{

    vector<unsigned int> diskPageIDs;

    unsigned int bufferPages = MEM_SIZE_IN_PAGE - 2;

    unsigned int inputPageIndex = 0;
    unsigned int outputPageIndex = 1;
    // range of buffer pages is now [2, MEM_SIZE_IN_PAGE -1)
    // unsigned int maxBuffPage = MEM_SIZE_IN_PAGE - 1;

    for (unsigned int i = 0; i < partitions.size(); ++i)
    {
        Bucket *bucket = &partitions[i];
        // if either the left or right have no records then there are no possible joinsd
        if (bucket->num_left_rel_record == 0 || bucket->num_right_rel_record == 0)
            continue;

        vector<unsigned int> smallRelationIndices;
        vector<unsigned int> largeRelationIndices;
        if (bucket->num_left_rel_record <= bucket->num_right_rel_record)
        {
            // we want to then load the left partition into memory table
            smallRelationIndices = bucket->get_left_rel();
            largeRelationIndices = bucket->get_right_rel();
        } else {
            smallRelationIndices = bucket->get_right_rel();
            largeRelationIndices = bucket->get_left_rel();
        }

        bool finalProbe = (i == partitions.size()- 1);
        //cout << "probe and " << i <<"\n";
        probeRelation(diskPageIDs, smallRelationIndices, largeRelationIndices, disk, mem, bufferPages, inputPageIndex,
        outputPageIndex, finalProbe);
        //break;
    }

    return diskPageIDs;
}


void probeRelation(vector<unsigned int> &diskPageIDs, vector<unsigned int> &smallRelationPageIndices, vector<unsigned int> &largeRelationPageIndices,
Disk *disk, Mem *mem, unsigned int bufferPages, unsigned int inputPageIndex, unsigned int outputPageIndex, bool finalProbe) {

    for (vector<unsigned int>::iterator smallRelpageIndex = smallRelationPageIndices.begin();
         smallRelpageIndex != smallRelationPageIndices.end(); ++smallRelpageIndex)
    {

        // load disk page to input page
        mem->loadFromDisk(disk, *smallRelpageIndex, inputPageIndex);
        Page *inputPage = mem->mem_page(inputPageIndex);

        // itterate over all records in inputPage and hash them into in memory hash table
        for (unsigned int recordIndex = 0; recordIndex < inputPage->size(); ++recordIndex)
        {
            Record record = inputPage->get_record(recordIndex);

            unsigned int bufferPageIndex = (record.probe_hash() % bufferPages) + 2; // add 2 since 0 and 1 are reserved for input and output
            Page *bufferPage = mem->mem_page(bufferPageIndex);
            bufferPage->loadRecord(record);

            //cout << "hash value: " << bufferPageIndex << " ";
            //record.print();
        }
    }
               // cout << "\n\n";



    // loop over the right relation pages and find matches
    for (unsigned int i = 0; i < largeRelationPageIndices.size(); ++i)
    {
        unsigned int rightRelPageIndex = largeRelationPageIndices[i];
       // cout << "A: " << i << " B: " << largeRelationPageIndices.size() << "\n";

        // load disk page to input page
        mem->loadFromDisk(disk, rightRelPageIndex, inputPageIndex);
        Page *inputPage = mem->mem_page(inputPageIndex);

        // loop through records on the input page and compute hash
        // see if there are matches and
        // load these pairs to the output page
        for (unsigned int inputPageRecordIndex = 0; inputPageRecordIndex < inputPage->size(); ++inputPageRecordIndex)
        {
            Record inputPageRecord = inputPage->get_record(inputPageRecordIndex);

            unsigned int bufferPageIndex = (inputPageRecord.probe_hash() % bufferPages) + 2;
            Page *bufferPage = mem->mem_page(bufferPageIndex);

           // cout << "input record with hash val " << bufferPageIndex << " ";
            //inputPageRecord.print();

            // loop through this buffer page and output record pairs to output page
            for (unsigned int bufferPageRecordIndex = 0; bufferPageRecordIndex < bufferPage->size(); ++bufferPageRecordIndex)
            {
               // cout << "A\n";
                Record bufferPageRecord = bufferPage->get_record(bufferPageRecordIndex);
                Page *outputPage = mem->mem_page(outputPageIndex);
                // load pair to outputpage
                outputPage->loadPair(inputPageRecord, bufferPageRecord);

                
                 //cout << "Checking if need to flush\n";

                // check if output page is full or if we are on the last page in the right relation, if it is flush to disk
                if (outputPage->size() == RECORDS_PER_PAGE)
                {
                    //cout << "flushing!\n";
                    unsigned int diskPageIndex = mem->flushToDisk(disk, outputPageIndex);
                    diskPageIDs.push_back(diskPageIndex);
                    // clear the output buffer page
                    outputPage->reset();
                }
            }
        }


        

      //  cout << "LOOP OVER\n";
    }
// clear the buffer pages
        for (unsigned int i = 2; i < MEM_SIZE_IN_PAGE - 1; ++i)
        {
            //cout << "CLEAR";
            Page *p = mem->mem_page(i);
            p->reset();
        }
    if(finalProbe) {
       // cout << "flushing!\n";
        Page *outputPage = mem->mem_page(outputPageIndex);
        unsigned int diskPageIndex = mem->flushToDisk(disk, outputPageIndex);
        diskPageIDs.push_back(diskPageIndex);
        // clear the output buffer page
        outputPage->reset();
    }
}

