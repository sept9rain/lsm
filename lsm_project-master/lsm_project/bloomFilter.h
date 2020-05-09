#ifndef _BLOOM_FILTER_
#define _BLOOM_FILTER_

/*
 * Note: Currently does not support union or intersection because following an
 *       intersection/union the size can only be approximated.
 *       Potential options include leaving them out, using the approximate size
 *       or removing size restriction on insert if a BloomFilter is the result
 *       of a union or intersection.
 */

#include "def.h"

#include <cmath>
#include <utility>
#include "murmurhash.h"

template <class T>
class BloomFilter {
private:
  unsigned char * volatile bitVector;
  unsigned int insertedCnt; // number of elements inserted
  unsigned int capacity; // total number of elements possible
  double errorRate; // user specified error rate

  unsigned int bitVectorSize; // size of data array
  std::list<int> hashSeeds;

  FILE* bff;

public:
	void load(const std::string& fn){
		bff = fopen((getEPath() + "/" + myPar::getInstance().dbName + "_bf").c_str(), "rb");
		if (!bff)
			return;
		if (!bitVector)
			delete[] bitVector;
		fread(&capacity, sizeof(capacity), 1, bff);
		fread(&errorRate, sizeof(errorRate), 1, bff);
		BloomFilter(capacity, errorRate);
		fread(&insertedCnt, sizeof(insertedCnt), 1, bff);
		fread(bitVector, sizeof(unsigned char), bitVectorSize / 8, bff);

		fclose(bff);
	}
	void save(const std::string& fn){
		bff = fopen((getEPath() + "/" + myPar::getInstance().dbName + "_bf").c_str(), "wb");
		if (!bff)
			return;
		fwrite(&capacity, sizeof(capacity), 1, bff);
		fwrite(&errorRate, sizeof(errorRate), 1, bff);
		fwrite(&insertedCnt, sizeof(insertedCnt), 1, bff);
		fwrite(bitVector, sizeof(unsigned char), bitVectorSize / 8, bff);
		fclose(bff);
	}

	BloomFilter() = delete;
	BloomFilter(unsigned int capacity, double errorRate);
	~BloomFilter();

  BloomFilter(BloomFilter &&rhs);
  BloomFilter<T>& operator=(BloomFilter&& rhs);

  // return true if full, false if not
  bool full() { return insertedCnt == capacity; }
  // return number of elements inserted
  unsigned int insertedCount() { return insertedCnt; }
  // clear bitVector
  void clear(){
		  insertedCnt = 0;
		  int size = bitVectorSize / 8;
		  //memset(bitVector, 0, size);
		  for (auto i = 0; i < size; i++){
			  bitVector[i] = 0;
		  }
  }

  // add element to bloom filter given data and sizeof(data);
  // return true if inserted, false if full
  bool insert(T &data);
  // add element to bloom filter given data of type T and a byteCnt
  // return true if inserted, false if full
  bool insert(T &data, int byteCnt);
  
  // checks for existence given data of type T and sizeof(data)
  bool exists(T &data);
  // checks for existance given data of type T and a byteCnt
  bool exists(T &data, int bytCnt);
};





// Constructor
template <class T>
BloomFilter<T>::BloomFilter(unsigned int capacity, double errorRate) : insertedCnt{0}, capacity{capacity}, errorRate{errorRate} {
  // calculate number of bits that will give desired error rate with desired capacity
  bitVectorSize = (unsigned int)ceil((capacity*-log(errorRate))/(log(2)*log(2)));
  
  unsigned int dataSize = bitVectorSize/8;
  if(bitVectorSize % 8)
    dataSize++;
  bitVector = new unsigned char[dataSize];

  bitVectorSize = dataSize * 8;

  // calculate number of functions -> use bitVectorSize * 8 insetad of numBits
  // because numBits may underestimate the size if bitVectorSize isn't divisible
  // by 8 (bits in a byte (char))
  int kk = (bitVectorSize/capacity) * log(2) + 0.5;
  hashSeeds.clear();
  for(int i=0; i<kk; i++) {
    // need to do some research to ensure sequential seeds won't cause issues
    // technically can store k instead and seed with [0..k) but want flexibility
    // for alternative seed list
    hashSeeds.push_back(i);
  }
}

// Move Constructor
template <class T>
BloomFilter<T>::BloomFilter(BloomFilter &&rhs): insertedCnt{rhs.insertedCnt}, capacity{rhs.capacity}, errorRate{rhs.errorRate}, bitVectorSize{rhs.bitVectorSize} {
  hashSeeds = std::move(rhs.hashSeeds);
  if (!bitVector){
	  delete[] bitVector;
  }
  bitVector = rhs.bitVector;
  rhs.bitVector = nullptr;
  insertedCnt = capacity = errorRate = bitVectorSize = 0;
}

// move = operator
template <class T>
BloomFilter<T>& BloomFilter<T>::operator=(BloomFilter &&rhs) {
  if(this != &rhs) {
    hashSeeds = std::move(rhs.hashSeeds);
	if (!bitVector){
		delete[] bitVector;
	}
    bitVector = rhs.bitVector;
    insertedCnt = rhs.insertedCnt;
    capacity = rhs.capacity;
    errorRate = rhs.errorRate;
    bitVectorSize = rhs.bitVectorSize;
    rhs.bitVector = nullptr;
    rhs.insertedCnt = rhs.capacity = rhs.errorRate = rhs.bitVectorSize = 0;
  }
  return *this;
}

template <class T>
BloomFilter<T>::~BloomFilter() {
	if (!bitVector){
		delete[]bitVector;
		bitVector = nullptr;
	}
}



// add element to bloom filter given data and sizeof(data);
// return true if inserted, false if full
template <class T>
bool BloomFilter<T>::insert(T &data) {
  return insert(data, sizeof(data));
}

// add element to bloom filter given data of type T and a byteCnt
// return true if inserted, false if full
template <class T>
bool BloomFilter<T>::insert(T &data, int byteCnt) {
  unsigned int hashVal, dataIdx, bitIdx;
  
  if(full()) {
    return false;
  }

  insertedCnt++;

  for(auto i : hashSeeds) {
    hashVal = murmur3_32(&data, byteCnt, i) % bitVectorSize;
    dataIdx = hashVal/8;
    bitIdx = hashVal % 8;

    bitVector[dataIdx] |= (1 << bitIdx);
  }

  return true;
}

// checks for existence given data of type T and sizeof(data)
template <class T>
bool BloomFilter<T>::exists(T &data) {
  return exists(data, sizeof(data));
}

// checks for existance given data of type T and a byteCnt
template <class T>
bool BloomFilter<T>::exists(T &data, int byteCnt) {
  unsigned int hashVal, dataIdx, bitIdx, hashResult;
  
  for(auto i : hashSeeds) {
    hashVal = murmur3_32(&data, byteCnt, i) % bitVectorSize;
    dataIdx = hashVal/8;
    bitIdx = hashVal % 8;

    hashResult = bitVector[dataIdx] & (1 << bitIdx);
    if(hashResult == 0)
      return false;
    
  }
  
  return true;
}


#endif
