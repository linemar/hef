#include <immintrin.h>
#include <stdint.h>



__attribute__((always_inline)) inline void  
hashjoin_i64_l1(const uint64_t *hash_t, const uint64_t hash_size, const uint64_t *id,  const uint64_t *val, const uint64_t in_size, const uint64_t batch_size, uint64_t *out_id, uint64_t &out_size)
{
uint64_t offset =  0;
const uint32_t r =  47;

const __m512i m_v = _mm512_set1_epi64(0xc6a4a7935bd1e995);
const int64_t m_s = 0xc6a4a7935bd1e995;
const __m512i h_v = _mm512_set1_epi64(0x8445d61a4e774912^(8*0xc6a4a7935bd1e995));
const int64_t h_s = 0x8445d61a4e774912^(8*0xc6a4a7935bd1e995);
const __m512i addr_mask_v = _mm512_set1_epi64(hash_size - 1);
const uint64_t addr_mask_s = hash_size - 1;
const uint64_t scale =  8;

__m512i kr_v0_p0;
__m512i kr_v1_p0;
int64_t kr_s0_p0;
int64_t kr_s1_p0;
__m512i kr_v0_p1;
__m512i kr_v1_p1;
int64_t kr_s0_p1;
int64_t kr_s1_p1;

__mmask8 mask_v0_p0;
__mmask8 mask_v1_p0;
bool mask_s0_p0;
bool mask_s1_p0;
__mmask8 mask_v0_p1;
__mmask8 mask_v1_p1;
bool mask_s0_p1;
bool mask_s1_p1;

__m512i data_v0_p0;
__m512i data_v1_p0;
uint64_t data_s0_p0;
uint64_t data_s1_p0;
__m512i data_v0_p1;
__m512i data_v1_p1;
uint64_t data_s0_p1;
uint64_t data_s1_p1;

__m512i k_v0_p0;
__m512i k_v1_p0;
uint64_t k_s0_p0;
uint64_t k_s1_p0;
__m512i k_v0_p1;
__m512i k_v1_p1;
uint64_t k_s0_p1;
uint64_t k_s1_p1;

__m512i dst_v0_p0;
__m512i dst_v1_p0;
uint64_t dst_s0_p0;
uint64_t dst_s1_p0;
__m512i dst_v0_p1;
__m512i dst_v1_p1;
uint64_t dst_s0_p1;
uint64_t dst_s1_p1;

__m512i addr_v0_p0;
__m512i addr_v1_p0;
uint64_t addr_s0_p0;
uint64_t addr_s1_p0;
__m512i addr_v0_p1;
__m512i addr_v1_p1;
uint64_t addr_s0_p1;
uint64_t addr_s1_p1;

__m512i vals_v0_p0;
__m512i vals_v1_p0;
uint64_t vals_s0_p0;
uint64_t vals_s1_p0;
__m512i vals_v0_p1;
__m512i vals_v1_p1;
uint64_t vals_s0_p1;
uint64_t vals_s1_p1;

__m512i hval_v0_p0;
__m512i hval_v1_p0;
uint64_t hval_s0_p0;
uint64_t hval_s1_p0;
__m512i hval_v0_p1;
__m512i hval_v1_p1;
uint64_t hval_s0_p1;
uint64_t hval_s1_p1;

__m512i ids_v0_p0;
__m512i ids_v1_p0;
uint64_t ids_s0_p0;
uint64_t ids_s1_p0;
__m512i ids_v0_p1;
__m512i ids_v1_p1;
uint64_t ids_s0_p1;
uint64_t ids_s1_p1;

for(uint64_t i = 0; i < in_size; i = i + batch_size)
{
data_v0_p0 = _mm512_loadu_epi64(val + offset + 0);
data_v1_p0 = _mm512_loadu_epi64(val + offset + 8);
data_s0_p0 = *(val + offset + 16);
data_s1_p0 = *(val + offset + 17);
data_v0_p1 = _mm512_loadu_epi64(val + offset + 18);
data_v1_p1 = _mm512_loadu_epi64(val + offset + 26);
data_s0_p1 = *(val + offset + 34);
data_s1_p1 = *(val + offset + 35);

k_v0_p0 = _mm512_mullo_epi64(data_v0_p0, m_v);
k_v1_p0 = _mm512_mullo_epi64(data_v1_p0, m_v);
k_s0_p0 = m_s * data_s0_p0;
k_s1_p0 = m_s * data_s1_p0;
k_v0_p1 = _mm512_mullo_epi64(data_v0_p1, m_v);
k_v1_p1 = _mm512_mullo_epi64(data_v1_p1, m_v);
k_s0_p1 = m_s * data_s0_p1;
k_s1_p1 = m_s * data_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(k_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(k_v1_p0, r);
kr_s0_p0 = k_s0_p0 >> r;
kr_s1_p0 = k_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(k_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(k_v1_p1, r);
kr_s0_p1 = k_s0_p1 >> r;
kr_s1_p1 = k_s1_p1 >> r;

kr_v0_p0 = _mm512_xor_epi64(kr_v0_p0, k_v0_p0);
kr_v1_p0 = _mm512_xor_epi64(kr_v1_p0, k_v1_p0);
kr_s0_p0 = kr_s0_p0 ^ k_s0_p0;
kr_s1_p0 = kr_s1_p0 ^ k_s1_p0;
kr_v0_p1 = _mm512_xor_epi64(kr_v0_p1, k_v0_p1);
kr_v1_p1 = _mm512_xor_epi64(kr_v1_p1, k_v1_p1);
kr_s0_p1 = kr_s0_p1 ^ k_s0_p1;
kr_s1_p1 = kr_s1_p1 ^ k_s1_p1;

kr_v0_p0 = _mm512_mullo_epi64(kr_v0_p0, m_v);
kr_v1_p0 = _mm512_mullo_epi64(kr_v1_p0, m_v);
kr_s0_p0 = m_s * kr_s0_p0;
kr_s1_p0 = m_s * kr_s1_p0;
kr_v0_p1 = _mm512_mullo_epi64(kr_v0_p1, m_v);
kr_v1_p1 = _mm512_mullo_epi64(kr_v1_p1, m_v);
kr_s0_p1 = m_s * kr_s0_p1;
kr_s1_p1 = m_s * kr_s1_p1;

hval_v0_p0 = _mm512_xor_epi64(h_v, kr_v0_p0);
hval_v1_p0 = _mm512_xor_epi64(h_v, kr_v1_p0);
hval_s0_p0 = h_s ^ kr_s0_p0;
hval_s1_p0 = h_s ^ kr_s1_p0;
hval_v0_p1 = _mm512_xor_epi64(h_v, kr_v0_p1);
hval_v1_p1 = _mm512_xor_epi64(h_v, kr_v1_p1);
hval_s0_p1 = h_s ^ kr_s0_p1;
hval_s1_p1 = h_s ^ kr_s1_p1;

hval_v0_p0 = _mm512_mullo_epi64(hval_v0_p0, m_v);
hval_v1_p0 = _mm512_mullo_epi64(hval_v1_p0, m_v);
hval_s0_p0 = m_s * hval_s0_p0;
hval_s1_p0 = m_s * hval_s1_p0;
hval_v0_p1 = _mm512_mullo_epi64(hval_v0_p1, m_v);
hval_v1_p1 = _mm512_mullo_epi64(hval_v1_p1, m_v);
hval_s0_p1 = m_s * hval_s0_p1;
hval_s1_p1 = m_s * hval_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(hval_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(hval_v1_p0, r);
kr_s0_p0 = hval_s0_p0 >> r;
kr_s1_p0 = hval_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(hval_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(hval_v1_p1, r);
kr_s0_p1 = hval_s0_p1 >> r;
kr_s1_p1 = hval_s1_p1 >> r;

hval_v0_p0 = _mm512_xor_epi64(hval_v0_p0, kr_v0_p0);
hval_v1_p0 = _mm512_xor_epi64(hval_v1_p0, kr_v1_p0);
hval_s0_p0 = hval_s0_p0 ^ kr_s0_p0;
hval_s1_p0 = hval_s1_p0 ^ kr_s1_p0;
hval_v0_p1 = _mm512_xor_epi64(hval_v0_p1, kr_v0_p1);
hval_v1_p1 = _mm512_xor_epi64(hval_v1_p1, kr_v1_p1);
hval_s0_p1 = hval_s0_p1 ^ kr_s0_p1;
hval_s1_p1 = hval_s1_p1 ^ kr_s1_p1;

hval_v0_p0 = _mm512_mullo_epi64(hval_v0_p0, m_v);
hval_v1_p0 = _mm512_mullo_epi64(hval_v1_p0, m_v);
hval_s0_p0 = m_s * hval_s0_p0;
hval_s1_p0 = m_s * hval_s1_p0;
hval_v0_p1 = _mm512_mullo_epi64(hval_v0_p1, m_v);
hval_v1_p1 = _mm512_mullo_epi64(hval_v1_p1, m_v);
hval_s0_p1 = m_s * hval_s0_p1;
hval_s1_p1 = m_s * hval_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(hval_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(hval_v1_p0, r);
kr_s0_p0 = hval_s0_p0 >> r;
kr_s1_p0 = hval_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(hval_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(hval_v1_p1, r);
kr_s0_p1 = hval_s0_p1 >> r;
kr_s1_p1 = hval_s1_p1 >> r;

dst_v0_p0 = _mm512_xor_epi64(hval_v0_p0, kr_v0_p0);
dst_v1_p0 = _mm512_xor_epi64(hval_v1_p0, kr_v1_p0);
dst_s0_p0 = hval_s0_p0 ^ kr_s0_p0;
dst_s1_p0 = hval_s1_p0 ^ kr_s1_p0;
dst_v0_p1 = _mm512_xor_epi64(hval_v0_p1, kr_v0_p1);
dst_v1_p1 = _mm512_xor_epi64(hval_v1_p1, kr_v1_p1);
dst_s0_p1 = hval_s0_p1 ^ kr_s0_p1;
dst_s1_p1 = hval_s1_p1 ^ kr_s1_p1;

addr_v0_p0 = _mm512_and_epi64(dst_v0_p0, addr_mask_v);
addr_v1_p0 = _mm512_and_epi64(dst_v1_p0, addr_mask_v);
addr_s0_p0 = dst_s0_p0 & addr_mask_s;
addr_s1_p0 = dst_s1_p0 & addr_mask_s;
addr_v0_p1 = _mm512_and_epi64(dst_v0_p1, addr_mask_v);
addr_v1_p1 = _mm512_and_epi64(dst_v1_p1, addr_mask_v);
addr_s0_p1 = dst_s0_p1 & addr_mask_s;
addr_s1_p1 = dst_s1_p1 & addr_mask_s;

vals_v0_p0 = _mm512_i64gather_epi64(addr_v0_p0, hash_t, scale);
vals_v1_p0 = _mm512_i64gather_epi64(addr_v1_p0, hash_t, scale);
vals_s0_p0 = *(hash_t + addr_s0_p0);
vals_s1_p0 = *(hash_t + addr_s1_p0);
vals_v0_p1 = _mm512_i64gather_epi64(addr_v0_p1, hash_t, scale);
vals_v1_p1 = _mm512_i64gather_epi64(addr_v1_p1, hash_t, scale);
vals_s0_p1 = *(hash_t + addr_s0_p1);
vals_s1_p1 = *(hash_t + addr_s1_p1);

mask_v0_p0 = _mm512_cmpeq_epi64_mask (vals_v0_p0, data_v0_p0);
mask_v1_p0 = _mm512_cmpeq_epi64_mask (vals_v1_p0, data_v1_p0);
mask_s0_p0 = vals_s0_p0 == data_s0_p0 ? 1 : 0;
mask_s1_p0 = vals_s1_p0 == data_s1_p0 ? 1 : 0;
mask_v0_p1 = _mm512_cmpeq_epi64_mask (vals_v0_p1, data_v0_p1);
mask_v1_p1 = _mm512_cmpeq_epi64_mask (vals_v1_p1, data_v1_p1);
mask_s0_p1 = vals_s0_p1 == data_s0_p1 ? 1 : 0;
mask_s1_p1 = vals_s1_p1 == data_s1_p1 ? 1 : 0;

ids_v0_p0 = _mm512_loadu_epi64(id + offset + 0);
ids_v1_p0 = _mm512_loadu_epi64(id + offset + 8);
ids_s0_p0 = *(id + offset + 16);
ids_s1_p0 = *(id + offset + 17);
ids_v0_p1 = _mm512_loadu_epi64(id + offset + 18);
ids_v1_p1 = _mm512_loadu_epi64(id + offset + 26);
ids_s0_p1 = *(id + offset + 34);
ids_s1_p1 = *(id + offset + 35);

_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v0_p0, ids_v0_p0);
out_size += _mm_popcnt_u64(mask_v0_p0);
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v1_p0, ids_v1_p0);
out_size += _mm_popcnt_u64(mask_v1_p0);
if(mask_s0_p0)
{out_id[out_size] = ids_s0_p0;
out_size++;};
if(mask_s1_p0)
{out_id[out_size] = ids_s1_p0;
out_size++;};
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v0_p1, ids_v0_p1);
out_size += _mm_popcnt_u64(mask_v0_p1);
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v1_p1, ids_v1_p1);
out_size += _mm_popcnt_u64(mask_v1_p1);
if(mask_s0_p1)
{out_id[out_size] = ids_s0_p1;
out_size++;};
if(mask_s1_p1)
{out_id[out_size] = ids_s1_p1;
out_size++;};

offset += batch_size;
}
}


__attribute__((always_inline)) inline void
hashjoin_gather_i64_l1(const uint64_t *hash_t, const uint64_t hash_size, const uint64_t *id, const uint64_t *val, const uint64_t in_size, const uint64_t batch_size, uint64_t *out_id, uint64_t &out_size)
{
uint64_t offset =  0;
const uint32_t r =  47;

const __m512i m_v = _mm512_set1_epi64(0xc6a4a7935bd1e995);
const int64_t m_s = 0xc6a4a7935bd1e995;
const __m512i h_v = _mm512_set1_epi64(0x8445d61a4e774912^(8*0xc6a4a7935bd1e995));
const int64_t h_s = 0x8445d61a4e774912^(8*0xc6a4a7935bd1e995);
const __m512i addr_mask_v = _mm512_set1_epi64(hash_size - 1);
const uint64_t addr_mask_s = hash_size - 1;
const uint64_t scale =  8;

__m512i kr_v0_p0;
__m512i kr_v1_p0;
int64_t kr_s0_p0;
int64_t kr_s1_p0;
__m512i kr_v0_p1;
__m512i kr_v1_p1;
int64_t kr_s0_p1;
int64_t kr_s1_p1;

__mmask8 mask_v0_p0;
__mmask8 mask_v1_p0;
bool mask_s0_p0;
bool mask_s1_p0;
__mmask8 mask_v0_p1;
__mmask8 mask_v1_p1;
bool mask_s0_p1;
bool mask_s1_p1;


    __m512i index_v0_p0;
   __m512i index_v1_p0;
    uint64_t index_s0_p0;
    uint64_t index_s1_p0;
    __m512i index_v0_p1;
   __m512i index_v1_p1;
    uint64_t index_s0_p1;
    uint64_t index_s1_p1;


__m512i data_v0_p0;
__m512i data_v1_p0;
uint64_t data_s0_p0;
uint64_t data_s1_p0;
__m512i data_v0_p1;
__m512i data_v1_p1;
uint64_t data_s0_p1;
uint64_t data_s1_p1;

__m512i k_v0_p0;
__m512i k_v1_p0;
uint64_t k_s0_p0;
uint64_t k_s1_p0;
__m512i k_v0_p1;
__m512i k_v1_p1;
uint64_t k_s0_p1;
uint64_t k_s1_p1;

__m512i dst_v0_p0;
__m512i dst_v1_p0;
uint64_t dst_s0_p0;
uint64_t dst_s1_p0;
__m512i dst_v0_p1;
__m512i dst_v1_p1;
uint64_t dst_s0_p1;
uint64_t dst_s1_p1;

__m512i addr_v0_p0;
__m512i addr_v1_p0;
uint64_t addr_s0_p0;
uint64_t addr_s1_p0;
__m512i addr_v0_p1;
__m512i addr_v1_p1;
uint64_t addr_s0_p1;
uint64_t addr_s1_p1;

__m512i vals_v0_p0;
__m512i vals_v1_p0;
uint64_t vals_s0_p0;
uint64_t vals_s1_p0;
__m512i vals_v0_p1;
__m512i vals_v1_p1;
uint64_t vals_s0_p1;
uint64_t vals_s1_p1;

__m512i hval_v0_p0;
__m512i hval_v1_p0;
uint64_t hval_s0_p0;
uint64_t hval_s1_p0;
__m512i hval_v0_p1;
__m512i hval_v1_p1;
uint64_t hval_s0_p1;
uint64_t hval_s1_p1;

__m512i ids_v0_p0;
__m512i ids_v1_p0;
uint64_t ids_s0_p0;
uint64_t ids_s1_p0;
__m512i ids_v0_p1;
__m512i ids_v1_p1;
uint64_t ids_s0_p1;
uint64_t ids_s1_p1;

for(uint64_t i = 0; i < in_size; i = i + batch_size)
{
        index_v0_p0 = _mm512_loadu_epi64(id + offset + 0);
        index_v1_p0 = _mm512_loadu_epi64(id + offset + 8);
        index_s0_p0 = *(id + offset + 16);
        index_s1_p0 = *(id + offset + 17);
        index_v0_p1 = _mm512_loadu_epi64(id + offset + 18);
        index_v1_p1 = _mm512_loadu_epi64(id + offset + 26);
        index_s0_p1 = *(id + offset + 34);
        index_s1_p1 = *(id + offset + 35);

        data_v0_p0 = _mm512_i64gather_epi64(index_v0_p0, val, 8);
        data_v1_p0 = _mm512_i64gather_epi64(index_v1_p0, val, 8);
        data_s0_p0 = *(val + index_s0_p0);
        data_s1_p0 = *(val + index_s1_p0);
        data_v0_p1 = _mm512_i64gather_epi64(index_v0_p1, val, 8);
        data_v1_p1 = _mm512_i64gather_epi64(index_v1_p1, val, 8);
        data_s0_p1 = *(val + index_s0_p1);
        data_s1_p1 = *(val + index_s1_p1);

        // data_v0_p0 = _mm512_i64gather_epi64(index_v0_p0, val, 8);
        // data_s0_p0 = *(val + index_s0_p0);
        // data_v0_p1 = _mm512_i64gather_epi64(index_v0_p1, val, 8);
        // data_s0_p1 = *(val + index_s0_p1);
        // data_v0_p2 = _mm512_i64gather_epi64(index_v0_p2, val, 8);
        // data_s0_p2 = *(val + index_s0_p2);

        // data_v0_p0 = _mm512_loadu_epi64(val + offset + 0);
        // data_v1_p0 = _mm512_loadu_epi64(val + offset + 8);
        // data_s0_p0 = *(val + offset + 16);
        // data_s1_p0 = *(val + offset + 17);
        // data_v0_p1 = _mm512_loadu_epi64(val + offset + 18);
        // data_v1_p1 = _mm512_loadu_epi64(val + offset + 26);
        // data_s0_p1 = *(val + offset + 34);
        // data_s1_p1 = *(val + offset + 35);

k_v0_p0 = _mm512_mullo_epi64(data_v0_p0, m_v);
k_v1_p0 = _mm512_mullo_epi64(data_v1_p0, m_v);
k_s0_p0 = m_s * data_s0_p0;
k_s1_p0 = m_s * data_s1_p0;
k_v0_p1 = _mm512_mullo_epi64(data_v0_p1, m_v);
k_v1_p1 = _mm512_mullo_epi64(data_v1_p1, m_v);
k_s0_p1 = m_s * data_s0_p1;
k_s1_p1 = m_s * data_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(k_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(k_v1_p0, r);
kr_s0_p0 = k_s0_p0 >> r;
kr_s1_p0 = k_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(k_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(k_v1_p1, r);
kr_s0_p1 = k_s0_p1 >> r;
kr_s1_p1 = k_s1_p1 >> r;

kr_v0_p0 = _mm512_xor_epi64(kr_v0_p0, k_v0_p0);
kr_v1_p0 = _mm512_xor_epi64(kr_v1_p0, k_v1_p0);
kr_s0_p0 = kr_s0_p0 ^ k_s0_p0;
kr_s1_p0 = kr_s1_p0 ^ k_s1_p0;
kr_v0_p1 = _mm512_xor_epi64(kr_v0_p1, k_v0_p1);
kr_v1_p1 = _mm512_xor_epi64(kr_v1_p1, k_v1_p1);
kr_s0_p1 = kr_s0_p1 ^ k_s0_p1;
kr_s1_p1 = kr_s1_p1 ^ k_s1_p1;

kr_v0_p0 = _mm512_mullo_epi64(kr_v0_p0, m_v);
kr_v1_p0 = _mm512_mullo_epi64(kr_v1_p0, m_v);
kr_s0_p0 = m_s * kr_s0_p0;
kr_s1_p0 = m_s * kr_s1_p0;
kr_v0_p1 = _mm512_mullo_epi64(kr_v0_p1, m_v);
kr_v1_p1 = _mm512_mullo_epi64(kr_v1_p1, m_v);
kr_s0_p1 = m_s * kr_s0_p1;
kr_s1_p1 = m_s * kr_s1_p1;

hval_v0_p0 = _mm512_xor_epi64(h_v, kr_v0_p0);
hval_v1_p0 = _mm512_xor_epi64(h_v, kr_v1_p0);
hval_s0_p0 = h_s ^ kr_s0_p0;
hval_s1_p0 = h_s ^ kr_s1_p0;
hval_v0_p1 = _mm512_xor_epi64(h_v, kr_v0_p1);
hval_v1_p1 = _mm512_xor_epi64(h_v, kr_v1_p1);
hval_s0_p1 = h_s ^ kr_s0_p1;
hval_s1_p1 = h_s ^ kr_s1_p1;

hval_v0_p0 = _mm512_mullo_epi64(hval_v0_p0, m_v);
hval_v1_p0 = _mm512_mullo_epi64(hval_v1_p0, m_v);
hval_s0_p0 = m_s * hval_s0_p0;
hval_s1_p0 = m_s * hval_s1_p0;
hval_v0_p1 = _mm512_mullo_epi64(hval_v0_p1, m_v);
hval_v1_p1 = _mm512_mullo_epi64(hval_v1_p1, m_v);
hval_s0_p1 = m_s * hval_s0_p1;
hval_s1_p1 = m_s * hval_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(hval_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(hval_v1_p0, r);
kr_s0_p0 = hval_s0_p0 >> r;
kr_s1_p0 = hval_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(hval_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(hval_v1_p1, r);
kr_s0_p1 = hval_s0_p1 >> r;
kr_s1_p1 = hval_s1_p1 >> r;

hval_v0_p0 = _mm512_xor_epi64(hval_v0_p0, kr_v0_p0);
hval_v1_p0 = _mm512_xor_epi64(hval_v1_p0, kr_v1_p0);
hval_s0_p0 = hval_s0_p0 ^ kr_s0_p0;
hval_s1_p0 = hval_s1_p0 ^ kr_s1_p0;
hval_v0_p1 = _mm512_xor_epi64(hval_v0_p1, kr_v0_p1);
hval_v1_p1 = _mm512_xor_epi64(hval_v1_p1, kr_v1_p1);
hval_s0_p1 = hval_s0_p1 ^ kr_s0_p1;
hval_s1_p1 = hval_s1_p1 ^ kr_s1_p1;

hval_v0_p0 = _mm512_mullo_epi64(hval_v0_p0, m_v);
hval_v1_p0 = _mm512_mullo_epi64(hval_v1_p0, m_v);
hval_s0_p0 = m_s * hval_s0_p0;
hval_s1_p0 = m_s * hval_s1_p0;
hval_v0_p1 = _mm512_mullo_epi64(hval_v0_p1, m_v);
hval_v1_p1 = _mm512_mullo_epi64(hval_v1_p1, m_v);
hval_s0_p1 = m_s * hval_s0_p1;
hval_s1_p1 = m_s * hval_s1_p1;

kr_v0_p0 = _mm512_srli_epi64(hval_v0_p0, r);
kr_v1_p0 = _mm512_srli_epi64(hval_v1_p0, r);
kr_s0_p0 = hval_s0_p0 >> r;
kr_s1_p0 = hval_s1_p0 >> r;
kr_v0_p1 = _mm512_srli_epi64(hval_v0_p1, r);
kr_v1_p1 = _mm512_srli_epi64(hval_v1_p1, r);
kr_s0_p1 = hval_s0_p1 >> r;
kr_s1_p1 = hval_s1_p1 >> r;

dst_v0_p0 = _mm512_xor_epi64(hval_v0_p0, kr_v0_p0);
dst_v1_p0 = _mm512_xor_epi64(hval_v1_p0, kr_v1_p0);
dst_s0_p0 = hval_s0_p0 ^ kr_s0_p0;
dst_s1_p0 = hval_s1_p0 ^ kr_s1_p0;
dst_v0_p1 = _mm512_xor_epi64(hval_v0_p1, kr_v0_p1);
dst_v1_p1 = _mm512_xor_epi64(hval_v1_p1, kr_v1_p1);
dst_s0_p1 = hval_s0_p1 ^ kr_s0_p1;
dst_s1_p1 = hval_s1_p1 ^ kr_s1_p1;

addr_v0_p0 = _mm512_and_epi64(dst_v0_p0, addr_mask_v);
addr_v1_p0 = _mm512_and_epi64(dst_v1_p0, addr_mask_v);
addr_s0_p0 = dst_s0_p0 & addr_mask_s;
addr_s1_p0 = dst_s1_p0 & addr_mask_s;
addr_v0_p1 = _mm512_and_epi64(dst_v0_p1, addr_mask_v);
addr_v1_p1 = _mm512_and_epi64(dst_v1_p1, addr_mask_v);
addr_s0_p1 = dst_s0_p1 & addr_mask_s;
addr_s1_p1 = dst_s1_p1 & addr_mask_s;

vals_v0_p0 = _mm512_i64gather_epi64(addr_v0_p0, hash_t, scale);
vals_v1_p0 = _mm512_i64gather_epi64(addr_v1_p0, hash_t, scale);
vals_s0_p0 = *(hash_t + addr_s0_p0);
vals_s1_p0 = *(hash_t + addr_s1_p0);
vals_v0_p1 = _mm512_i64gather_epi64(addr_v0_p1, hash_t, scale);
vals_v1_p1 = _mm512_i64gather_epi64(addr_v1_p1, hash_t, scale);
vals_s0_p1 = *(hash_t + addr_s0_p1);
vals_s1_p1 = *(hash_t + addr_s1_p1);

mask_v0_p0 = _mm512_cmpeq_epi64_mask (vals_v0_p0, data_v0_p0);
mask_v1_p0 = _mm512_cmpeq_epi64_mask (vals_v1_p0, data_v1_p0);
mask_s0_p0 = vals_s0_p0 == data_s0_p0 ? 1 : 0;
mask_s1_p0 = vals_s1_p0 == data_s1_p0 ? 1 : 0;
mask_v0_p1 = _mm512_cmpeq_epi64_mask (vals_v0_p1, data_v0_p1);
mask_v1_p1 = _mm512_cmpeq_epi64_mask (vals_v1_p1, data_v1_p1);
mask_s0_p1 = vals_s0_p1 == data_s0_p1 ? 1 : 0;
mask_s1_p1 = vals_s1_p1 == data_s1_p1 ? 1 : 0;

ids_v0_p0 = _mm512_loadu_epi64(id + offset + 0);
ids_v1_p0 = _mm512_loadu_epi64(id + offset + 8);
ids_s0_p0 = *(id + offset + 16);
ids_s1_p0 = *(id + offset + 17);
ids_v0_p1 = _mm512_loadu_epi64(id + offset + 18);
ids_v1_p1 = _mm512_loadu_epi64(id + offset + 26);
ids_s0_p1 = *(id + offset + 34);
ids_s1_p1 = *(id + offset + 35);

_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v0_p0, ids_v0_p0);
out_size += _mm_popcnt_u64(mask_v0_p0);
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v1_p0, ids_v1_p0);
out_size += _mm_popcnt_u64(mask_v1_p0);
if(mask_s0_p0)
{out_id[out_size] = ids_s0_p0;
out_size++;};
if(mask_s1_p0)
{out_id[out_size] = ids_s1_p0;
out_size++;};
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v0_p1, ids_v0_p1);
out_size += _mm_popcnt_u64(mask_v0_p1);
_mm512_mask_compressstoreu_epi64(out_id + out_size, mask_v1_p1, ids_v1_p1);
out_size += _mm_popcnt_u64(mask_v1_p1);
if(mask_s0_p1)
{out_id[out_size] = ids_s0_p1;
out_size++;};
if(mask_s1_p1)
{out_id[out_size] = ids_s1_p1;
out_size++;};

offset += batch_size;
}
}
