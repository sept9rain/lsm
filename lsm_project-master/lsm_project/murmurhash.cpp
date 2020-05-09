#include <cstdint>
#include "murmurhash.h"
// built for little endian systems

inline uint32_t shift(uint32_t x, uint8_t shiftAmount) {
  return (x << shiftAmount) | (x >> (32-shiftAmount));
}

uint32_t murmur3_32(void *key, int length, uint32_t seed) {
  const uint8_t *data = (const uint8_t*)key;
  const int blockCnt = length/4;
  uint32_t k;
  uint32_t c1 = 0xcc9e2d51;
  uint32_t c2 = 0x1b873593;
  uint32_t hash = seed;


  // hash all (complete) 4 byte sections
  const uint32_t *fourByteChunk = (const uint32_t*)data;
  for(int i=0; i<blockCnt; i++) {
    k = fourByteChunk[i];
    k *= c1;
    k = shift(k, 15);
    k *= c2;

    hash ^= k;
    hash = shift(hash, 13);
    hash = hash * 5 + 0xe6546b64;
  }


  // handle remaining (0-3) bytes
  const uint8_t *endBits = (const uint8_t*)(data+blockCnt*4);
  uint32_t tmp = 0;
  // len%4 is equivalent to len&3 but len&3 should be quicker
  switch(length&3) {
    case 3: tmp |= endBits[2] << 16;
    case 2: tmp |= endBits[1] << 8;
    case 1: tmp |= endBits[0];
  };

  tmp *= c1;
  tmp = shift(tmp, 15);
  tmp *= c2;
  hash ^= tmp;

  hash ^= length;
  hash ^= hash >> 16;
  hash *= 0x85ebca6b;
  hash ^= hash >> 13;
  hash *= 0xc2b2ae35;
  hash ^= hash >> 16;

  return hash;
}
