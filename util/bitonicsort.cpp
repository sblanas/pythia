
/* 
 * Copyright 2014, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdint.h>
#include <xmmintrin.h>
#include <smmintrin.h>
#include <string.h>


#define CACHE_SIZE (4*1024)
#define NUM_KEYS (1024*1024*128)


//assuming
//    uint32_t key
//    uint32_t rid
typedef struct tag_record_t {
  int32_t rid;
  int32_t key;
} record_t;

#define GET_KEY(rec) (rec).key
#define GET_RID(rec) (rec).rid


static record_t * simd_merge_sort(record_t *input, record_t *output, uint64_t size);

void dobitonicsort(record_t* table, unsigned long tuples)
{
    record_t *output;
    record_t *buffer;

	buffer = (record_t*)numaallocate_local("BSrt", tuples * sizeof(record_t), NULL);

	output = simd_merge_sort(table, buffer, tuples);

	if (output == buffer)
	{
		memcpy(table, buffer, tuples*sizeof(record_t));
	}

	numadeallocate(buffer);
}

int cmp(const void *a, const void *b)
{
  return ( GET_KEY(*(record_t*)a) - GET_KEY(*(record_t*)b));
}


static inline  __m128d KEY_RID_MIN(__m128d a, __m128d b)
{
  __m128i a_int = (__m128i)a;
  __m128i b_int = (__m128i)b;

  __m128i mask = _mm_cmpgt_epi32(a_int, b_int);
  return _mm_blendv_pd(a, b, (__m128d)mask);
}

static inline  __m128d KEY_RID_MAX(__m128d a, __m128d b)
{
  __m128i a_int = (__m128i)a;
  __m128i b_int = (__m128i)b;

  __m128i mask = _mm_cmpgt_epi32(b_int, a_int);
  return _mm_blendv_pd(a, b, (__m128d)mask);
}


static void inline simd_bitonic_sort_16(double *input, double* output)
{
  __m128d x0_0, x0_1, x1_0, x1_1, x2_0, x2_1, x3_0, x3_1;

  x0_0 = _mm_load_pd(&input[0]);
  x0_1 = _mm_load_pd(&input[2]);
  x1_0 = _mm_load_pd(&input[4]);
  x1_1 = _mm_load_pd(&input[6]);
  x2_0 = _mm_load_pd(&input[8]);
  x2_1 = _mm_load_pd(&input[10]);
  x3_0 = _mm_load_pd(&input[12]);
  x3_1 = _mm_load_pd(&input[14]);

  __m128d min0_0 = KEY_RID_MIN(x0_0, x1_0);
  __m128d min0_1 = KEY_RID_MIN(x0_1, x1_1);
  __m128d max0_0 = KEY_RID_MAX(x0_0, x1_0);
  __m128d max0_1 = KEY_RID_MAX(x0_1, x1_1);
  __m128d min1_0 = KEY_RID_MIN(x2_0, x3_0);
  __m128d min1_1 = KEY_RID_MIN(x2_1, x3_1);
  __m128d max1_0 = KEY_RID_MAX(x2_0, x3_0);
  __m128d max1_1 = KEY_RID_MAX(x2_1, x3_1);


  __m128d min2_0 = KEY_RID_MIN(min0_0, min1_0);
  __m128d min2_1 = KEY_RID_MIN(min0_1, min1_1);
  __m128d max2_0 = KEY_RID_MAX(min0_0, min1_0);
  __m128d max2_1 = KEY_RID_MAX(min0_1, min1_1);

  __m128d min3_0 = KEY_RID_MIN(max0_0, max1_0);
  __m128d min3_1 = KEY_RID_MIN(max0_1, max1_1);
  __m128d max3_0 = KEY_RID_MAX(max0_0, max1_0);
  __m128d max3_1 = KEY_RID_MAX(max0_1, max1_1);

  __m128d min4_0 = KEY_RID_MIN(min3_0, max2_0);
  __m128d min4_1 = KEY_RID_MIN(min3_1, max2_1);
  __m128d max4_0 = KEY_RID_MAX(min3_0, max2_0);
  __m128d max4_1 = KEY_RID_MAX(min3_1, max2_1);

  //sorted output is in
  // min2 min4 max4 max3
   x0_0 = (__m128d)min2_0;
   x0_1 = (__m128d)min2_1;
   x1_0 = (__m128d)min4_0;
   x1_1 = (__m128d)min4_1;
   x2_0 = (__m128d)max4_0;
   x2_1 = (__m128d)max4_1;
   x3_0 = (__m128d)max3_0;
   x3_1 = (__m128d)max3_1;

  //transpose
  __m128d y0_0 = _mm_shuffle_pd(x0_0, x1_0, _MM_SHUFFLE2(0,0));
  __m128d y0_1 = _mm_shuffle_pd(x2_0, x3_0, _MM_SHUFFLE2(0,0));
  __m128d y1_0 = _mm_shuffle_pd(x0_0, x1_0, _MM_SHUFFLE2(1,1));
  __m128d y1_1 = _mm_shuffle_pd(x2_0, x3_0, _MM_SHUFFLE2(1,1));
  __m128d y2_0 = _mm_shuffle_pd(x0_1, x1_1, _MM_SHUFFLE2(0,0));
  __m128d y2_1 = _mm_shuffle_pd(x2_1, x3_1, _MM_SHUFFLE2(0,0));
  __m128d y3_0 = _mm_shuffle_pd(x0_1, x1_1, _MM_SHUFFLE2(1,1));
  __m128d y3_1 = _mm_shuffle_pd(x2_1, x3_1, _MM_SHUFFLE2(1,1));

  //store back
  _mm_store_pd(&output[0],  y0_0);
  _mm_store_pd(&output[2],  y0_1);
  _mm_store_pd(&output[4],  y1_0);
  _mm_store_pd(&output[6],  y1_1);
  _mm_store_pd(&output[8],  y2_0);
  _mm_store_pd(&output[10], y2_1);
  _mm_store_pd(&output[12], y3_0);
  _mm_store_pd(&output[14], y3_1);

}


static void inline
bitonic_merge_kernel(__m128d *a_0,
                     __m128d *a_1,
                     __m128d *b_0,
                     __m128d *b_1)
{

  __m128d ta_0 = (__m128d)*a_0;
  __m128d ta_1 = (__m128d)*a_1;

  __m128d tb_0 = (__m128d)*b_1;
  __m128d tb_1 = (__m128d)*b_0;


  __m128d l_0, l_1, h_0, h_1; 

  //reverse b
  tb_0 = _mm_shuffle_pd(tb_0, tb_0, _MM_SHUFFLE2(0,1));
  tb_1 = _mm_shuffle_pd(tb_1, tb_1, _MM_SHUFFLE2(0,1));



  //level 1
  /*
  printf("level1::\n");
  print_m128d("ta_0", ta_0);
  print_m128d("ta_1", ta_1);
  print_m128d("tb_0", tb_0);
  print_m128d("tb_1", tb_1);
  printf("\n");
  */
  l_0 = KEY_RID_MIN(ta_0, tb_0);
  l_1 = KEY_RID_MIN(ta_1, tb_1);
  h_0 = KEY_RID_MAX(ta_0, tb_0);
  h_1 = KEY_RID_MAX(ta_1, tb_1);

  /*
  print_m128d("l_0", l_0);
  print_m128d("l_1", l_1);
  print_m128d("h_0", h_0);
  print_m128d("h_1", h_1);
  printf("\n");
  */
  ta_0 = l_1;
  ta_1 = h_1;

  tb_0 = l_0;
  tb_1 = h_0;

  //level 2
  /*
  printf("level2::\n");
  print_m128d("ta_0", ta_0);
  print_m128d("ta_1", ta_1);
  print_m128d("tb_0", tb_0);
  print_m128d("tb_1", tb_1);
  printf("\n");
  */
  l_0 = KEY_RID_MIN(ta_0, tb_0);
  l_1 = KEY_RID_MIN(ta_1, tb_1);
  h_0 = KEY_RID_MAX(ta_0, tb_0);
  h_1 = KEY_RID_MAX(ta_1, tb_1);
  /*
  print_m128d("l_0", l_0);
  print_m128d("l_1", l_1);
  print_m128d("h_0", h_0);
  print_m128d("h_1", h_1);
  printf("\n");
  */
  ta_0 = _mm_shuffle_pd(l_0, h_0, _MM_SHUFFLE2(1,1));
  ta_1 = _mm_shuffle_pd(l_1, h_1, _MM_SHUFFLE2(1,1));
  tb_0 = _mm_shuffle_pd(l_0, h_0, _MM_SHUFFLE2(0,0));
  tb_1 = _mm_shuffle_pd(l_1, h_1, _MM_SHUFFLE2(0,0));

  //level 3
  /*
  printf("level3::\n");
  print_m128d("ta_0", ta_0);
  print_m128d("ta_1", ta_1);
  print_m128d("tb_0", tb_0);
  print_m128d("tb_1", tb_1);
  printf("\n");
  */
  l_0 = KEY_RID_MIN(ta_0, tb_0);
  l_1 = KEY_RID_MIN(ta_1, tb_1);
  h_0 = KEY_RID_MAX(ta_0, tb_0);
  h_1 = KEY_RID_MAX(ta_1, tb_1);

  /*
  print_m128d("l_0", l_0);
  print_m128d("l_1", l_1);
  print_m128d("h_0", h_0);
  print_m128d("h_1", h_1);
  printf("\n");
  */
  ta_0 = _mm_shuffle_pd(l_0, h_0, _MM_SHUFFLE2(0,0));
  ta_1 = _mm_shuffle_pd(l_0, h_0, _MM_SHUFFLE2(1,1));
  tb_0 = _mm_shuffle_pd(l_1, h_1, _MM_SHUFFLE2(0,0));
  tb_1 = _mm_shuffle_pd(l_1, h_1, _MM_SHUFFLE2(1,1));

  /*
  print_m128d("ta_0", ta_0);
  print_m128d("ta_1", ta_1);
  print_m128d("tb_0", tb_0);
  print_m128d("tb_1", tb_1);
  */
  //output
  *a_0 = (__m128d) ta_0;
  *a_1 = (__m128d) ta_1;
  *b_0 = (__m128d) tb_0;
  *b_1 = (__m128d) tb_1;

}


void
simd_merge(record_t *output, record_t *input, uint64_t size)
{

  uint64_t half;
  record_t *halfptr;
  record_t *endptr;

  half = size / 2;
  halfptr = input + half;
  endptr = input + size;

  //printf("half := %d, halfptr := %p end_ptr := %p\n",
  //       half, halfptr, endptr);

  record_t *list1 = input;
  record_t *list2 = halfptr;

  __m128d vec_x_0 = _mm_load_pd((double*)&list1[0]);
  __m128d vec_x_1 = _mm_load_pd((double*)&list1[2]);
  __m128d vec_y_0 = _mm_load_pd((double*)&list2[0]);
  __m128d vec_y_1 = _mm_load_pd((double*)&list2[2]);

  list1 += 4;
  list2 += 4;

  bitonic_merge_kernel(&vec_x_0, &vec_x_1, &vec_y_0, &vec_y_1);


  _mm_store_pd((double*)&output[0], vec_x_0);
  _mm_store_pd((double*)&output[2], vec_x_1);
  output += 4;

  while(list1 < halfptr && list2 < endptr) {
    if (GET_KEY(*list1) < GET_KEY(*list2)) {
      vec_x_0 = _mm_load_pd((double*)&list1[0]);
      vec_x_1 = _mm_load_pd((double*)&list1[2]);
      list1 += 4;
    } else {
      vec_x_0 = _mm_load_pd((double*)&list2[0]);
      vec_x_1 = _mm_load_pd((double*)&list2[2]);
      list2 += 4;
    }

    bitonic_merge_kernel(&vec_x_0, &vec_x_1, &vec_y_0, &vec_y_1);
    _mm_store_pd((double*)&output[0], vec_x_0);
    _mm_store_pd((double*)&output[2], vec_x_1);

    output += 4;
  }


  while(list1 < halfptr){
    vec_x_0 = _mm_load_pd((double*)&list1[0]);
    vec_x_1 = _mm_load_pd((double*)&list1[2]);
    list1 += 4;
    bitonic_merge_kernel(&vec_x_0, &vec_x_1, &vec_y_0, &vec_y_1);
    _mm_store_pd((double*)&output[0], vec_x_0);
    _mm_store_pd((double*)&output[2], vec_x_1);
    output += 4;
  }


  while(list2 < endptr){
    vec_x_0 = _mm_load_pd((double*)&list2[0]);
    vec_x_1 = _mm_load_pd((double*)&list2[2]);
    list2 += 4;

    bitonic_merge_kernel(&vec_x_0, &vec_x_1, &vec_y_0, &vec_y_1);
    _mm_store_pd((double*)&output[0], vec_x_0);
    _mm_store_pd((double*)&output[2], vec_x_1);


    output += 4;
  }

  _mm_store_pd((double*)&output[0], vec_y_0);
  _mm_store_pd((double*)&output[2], vec_y_1);



}

void simd_merge_sort_cache(record_t *input,
                           record_t *output,
                           uint64_t size)
{

  uint64_t i;

  int lg = __builtin_ctz(size);

  record_t* Src;
  record_t* Tgt;

  // create 4 wide sorted sequences
  if ((lg & 0x1) == 0) {
    for (i = 0; i < size; i += 16)
      simd_bitonic_sort_16((double*)&input[i], (double*)&output[i]);
    Src = output;
    Tgt = input;
  } else {
    for (i = 0; i < size; i += 16)
      simd_bitonic_sort_16((double*)&input[i], (double*)&input[i]);
    Src = input;
    Tgt = output;
  }


  uint64_t merge_size = 4;
  while(merge_size < size) {
    uint64_t k;

    merge_size *= 2;
    for (k = 0; k < size; k += merge_size) {
      simd_merge(&Tgt[k], &Src[k], merge_size);
    }
    record_t* tmp = Tgt;
    Tgt = Src;
    Src = tmp;
  }

}


record_t * simd_merge_sort(record_t *input,
                           record_t *output, uint64_t size)
{

  const uint64_t Cache_Size = CACHE_SIZE;
  assert(size % Cache_Size == 0);
  if (size < Cache_Size) {
    simd_merge_sort_cache(input, output, size);
    return output;
  }
  uint64_t i = 0;
  for (i = 0; i < size; i+= Cache_Size) {
    simd_merge_sort_cache(&input[i], &output[i], Cache_Size);
  }

  record_t *Src = output;
  record_t *Tgt = input;

  uint64_t merge_size = Cache_Size;
  while(merge_size < size) {
    uint64_t k;
    merge_size *= 2;
    for (k = 0; k < size; k+= merge_size) {
      simd_merge(&Tgt[k], &Src[k], merge_size);
    }
    record_t *tmp = Tgt;
    Tgt = Src;
    Src = tmp;
  }
  return Src;
}
