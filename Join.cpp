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


    void partitionRelation(unsigned int begin, unsigned int end, Disk *disk, Mem* mem,
    unsigned int inputPageIndex, unsigned int bufferPages, vector<Bucket> &buckets, int rightOrLeft);

vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) {
        
        //1 page for input
        //B-1 pages for partions

        unsigned int inputPageIndex = 0;
        unsigned int bufferPages = MEM_SIZE_IN_PAGE - 1; 


        //the amount of buckets is proprotional to the amount of buffer pages
        vector<Bucket> buckets(bufferPages, Bucket(disk));
        
        partitionRelation(left_rel.first, left_rel.second, disk, mem, inputPageIndex, bufferPages, buckets, 1);
        partitionRelation(right_rel.first, right_rel.second, disk, mem, inputPageIndex, bufferPages, buckets, 0);

        for(unsigned int i = 0; i < buckets.size(); ++i) {

            vector<unsigned int> leftDiskPageIds = buckets[i].get_left_rel();
            vector<unsigned int> rightDiskPages = buckets[i].get_right_rel();
            // for(int x = 0; x < leftDiskPageIds.size(); ++x) {
            //     cout << "Bucket " << i << " " << leftDiskPageIds[x] << "\n";
            // }
            
            //vector<unsigned int> rightDiskPageIds = buckets[i].get_right_rel();
            cout << "partition: " << i << "\n";
            cout << "left relation records:\n";
            for(vector<unsigned int>::iterator it = leftDiskPageIds.begin(); it != leftDiskPageIds.end(); ++it) {
                mem->loadFromDisk(disk, *it, 0);
                Page *p = mem->mem_page(0);
                for(int i = 0; i < p->size(); ++i) {
                    Record r = p->get_record(i);
                    r.print();
                }
            }
            cout << "right relation records:\n";
            for(vector<unsigned int>::iterator it = rightDiskPages.begin(); it != rightDiskPages.end(); ++it) {
                mem->loadFromDisk(disk, *it, 0);
                Page *p = mem->mem_page(0);
                for(int i = 0; i < p->size(); ++i) {
                    Record r = p->get_record(i);
                    r.print();
                }
            }

            cout <<"\n\n";

            
        }

        return buckets; 
    }

    void partitionRelation(unsigned int begin, unsigned int end, Disk *disk, Mem* mem,
    unsigned int inputPageIndex, unsigned int bufferPages, vector<Bucket> &buckets, int rightOrLeft) {


        //partion left relation
        //we want to iterate over every disk page in the relation
        for(unsigned int diskPageIndex = begin; diskPageIndex < end; ++diskPageIndex) {

            //load the page from disk to inputpage
            mem->loadFromDisk(disk, diskPageIndex, inputPageIndex);
            Page* inputPage = mem->mem_page(inputPageIndex);

            //hash each record in this page 
            //data record returned from get_record will be in range of [0, size)
            for(unsigned int recordIndex = 0; recordIndex < inputPage->size(); ++recordIndex) {
                //cout << "HERE" << "\n";

                Record record = inputPage->get_record(recordIndex);
               // record.print();
                unsigned int hashVal = (record.partition_hash() % bufferPages) + 1; //add 1 since page 0 reserved for input
                //cout << hashVal << "\n\n";

                //now load this record into buffer page from the hashVal
                Page* bufferPage = mem->mem_page(hashVal);
                bufferPage->loadRecord(record);

                //check if buffer page is full or we are in last itteration, and flush all buffer pages to disk if it is
                if(bufferPage->size() == RECORDS_PER_PAGE || diskPageIndex == end - 1) {

                    //loop over every buffer page
                    //bufferPageIndex starts = 1 since 0 is reserved for inputPage
                    for(unsigned int bufferPageIndex = 1; bufferPageIndex <= bufferPages; ++bufferPageIndex) {

                        unsigned int diskpage = mem->flushToDisk(disk, bufferPageIndex);

                        if(rightOrLeft == 0) {
                             buckets[bufferPageIndex - 1].add_right_rel_page(diskpage); //-1 since the buckets array is 0 indexed
                        } else if(rightOrLeft == 1) {
                            buckets[bufferPageIndex - 1].add_left_rel_page(diskpage);
                        }

                        //clear the page since we flushed to disk
                        Page *p = mem->mem_page(bufferPageIndex);
                        p->reset();
                    }


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
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<unsigned int> x;
    return x;
}



