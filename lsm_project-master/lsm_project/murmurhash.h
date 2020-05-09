#ifndef _MURMUR_HASH_
#define _MURMUR_HASH_

// built for little endian systems

inline uint32_t shift(uint32_t x, uint8_t shiftAmount);

// 32 bit version of murmur hash 3
uint32_t murmur3_32(void *key, int length, uint32_t seed);

#endif
