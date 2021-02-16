// Copyright (c) 2016, the Dartino project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE.md file.
//
// Copyright (C) 2019 Toitware ApS. All rights reserved.

#include "cmpctmalloc.h"

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(TEST_CMPCTMALLOC) || defined (CMPCTMALLOC_ON_LINUX)

#include <limits.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <pthread.h>

#define LTRACEF(...)
#define LTRACE_ENTRY
#define DEBUG_ASSERT assert
#define ASSERT(x) if (!(x)) abort()
#define USE(x) ((void)(x))
#define STATIC_ASSERT(condition)
#define dprintf(...) fprintf(__VA_ARGS__)
#define INFO stderr
#define IRAM_ATTR

#ifdef CMPCTMALLOC_ON_LINUX
#define MULTI_HEAP_LOCK(x) while(!__sync_bool_compare_and_swap(&(x), NULL, (void *)(&(x))))
#define MULTI_HEAP_UNLOCK(x) __sync_bool_compare_and_swap(&(x), (void *)(&(x)), NULL)
#else
#define MULTI_HEAP_LOCK(x)
#define MULTI_HEAP_UNLOCK(x)
#endif

// For testing we just use a static variable for the current tag.
#define GET_THREAD_LOCAL_TAG pthread_getspecific(tls_key)
static pthread_key_t tls_key;

typedef struct multi_heap_info cmpct_heap_t;

#else  // TEST_CMPCTMALLOC

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "xtensa/core-macros.h"

#include "../../multi_heap_platform.h"

typedef struct multi_heap_info cmpct_heap_t;

void *multi_heap_malloc(cmpct_heap_t *heap, size_t size)
    __attribute__((alias("cmpct_malloc_impl")));

void multi_heap_free(cmpct_heap_t *heap, void *p)
    __attribute__((alias("cmpct_free_impl")));

void *multi_heap_realloc(cmpct_heap_t *heap, void *p, size_t size)
    __attribute__((alias("cmpct_realloc_impl")));

void *multi_heap_aligned_alloc(cmpct_heap_t *heap, size_t size, size_t alignment)
    __attribute__((alias("cmpct_aligned_alloc_impl")));

void multi_heap_aligned_free(cmpct_heap_t *heap, void *p)
    __attribute__((alias("cmpct_free_impl")));

size_t multi_heap_get_allocated_size(cmpct_heap_t *heap, void *p)
    __attribute__((alias("cmpct_get_allocated_size_impl")));

cmpct_heap_t *multi_heap_register(void *start, size_t size)
    __attribute__((alias("cmpct_register_impl")));

void multi_heap_get_info(cmpct_heap_t *heap, multi_heap_info_t *info)
    __attribute__((alias("cmpct_get_info_impl")));

size_t multi_heap_free_size(cmpct_heap_t *heap)
    __attribute__((alias("cmpct_free_size_impl")));

size_t multi_heap_minimum_free_size(cmpct_heap_t *heap)
    __attribute__((alias("cmpct_minimum_free_size_impl")));

void multi_heap_iterate_tagged_memory_areas(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback, int flags)
    __attribute__((alias("cmpct_iterate_tagged_memory_areas")));

void multi_heap_set_option(cmpct_heap_t *heap, int option, void *value)
    __attribute__((alias("cmpct_set_option")));

typedef uintptr_t addr_t;
typedef uintptr_t vaddr_t;

#define LTRACEF(...)
#define LTRACE_ENTRY
#define DEBUG_ASSERT assert
#define ASSERT(x) do {} while(false)
#define USE(x) ((void)(x))
#define STATIC_ASSERT(condition)
#define dprintf(...) fprintf(__VA_ARGS__)
#define INFO stdout

#ifdef __XTENSA__
IRAM_ATTR inline static bool in_interrupt_service_routine()
{
    int ps_register;
    __asm__ __volatile__("rsr.ps %0" : "=a"(ps_register));
    const int interrupt_priority_mask = 0xf;
    // During an interrupt the interrupt level is raised so that interrupts
    // below a certain level are masked.  We have seen an interrupt level of 3
    // (interrupt levels 0-3 are masked) while running normal code, so we now
    // require a higher level to indicate that we are calling malloc from an
    // interrupt, which is deprecated but may still happen.  We haven't seen it
    // recently.
    bool result = (ps_register & interrupt_priority_mask) > 3;
    return result;
}

static int first_allocations = true;

// First allocation is too early in the boot process to get a thread local data, so we skip that.
#define GET_THREAD_LOCAL_TAG ((in_interrupt_service_routine() || first_allocations) ? NULL : pvTaskGetThreadLocalStoragePointer(NULL, MULTI_HEAP_THREAD_TAG_INDEX))
#else
#define GET_THREAD_LOCAL_TAG (pvTaskGetThreadLocalStoragePointer(NULL, MULTI_HEAP_THREAD_TAG_INDEX))
#endif

#endif  // TEST_CMPCTMALLOC

#define ROUND_UP(x, alignment) (((x) + (alignment) - 1) & ~((alignment) - 1))
#define ROUND_DOWN(x, alignment) ((x) & ~((alignment) - 1))
#define IS_ALIGNED(x, alignment) (((x) & ((alignment) - 1)) == 0)
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))
// Provoke crash.
#define FATAL(reason) abort()
#define INLINE __attribute__((always_inline)) inline

// This is a two layer allocator.  Allocations that are a multiple of 4k in
// size or > 16336 bytes are allocated from a block allocator that gives out 4k
// aligned blocks.  Those less than 16336 bytes are allocated from cmpctmalloc.
// All allocations requesting a size divisible by 4k are fulfilled with
// addresses that are 4k aligned.

// Malloc implementation tuned for space.
//
// Allocation strategy takes place with a global mutex.  Freelist entries are
// kept in linked lists with 8 different sizes per binary order of magnitude
// and the header size is two words with eager coalescing on free.

void *cmpct_alloc(cmpct_heap_t *heap, size_t size);
void cmpct_free(cmpct_heap_t *heap, void *payload);
size_t cmpct_free_size_impl(cmpct_heap_t *heap);
size_t cmpct_get_allocated_size_impl(cmpct_heap_t *heap, void *p);
void cmpct_set_option(cmpct_heap_t *heap, int option, void *value);
static void *page_alloc(cmpct_heap_t *heap, intptr_t pages, void *tag);
static void page_free(cmpct_heap_t *heap, void *address, int pages_dummy);
struct header_struct;
struct free_struct;
static inline struct header_struct *right_header(struct header_struct *header);
static inline struct header_struct *left_header(struct header_struct *header);
static size_t page_number(cmpct_heap_t *heap, void *p);
static void *allocation_tail(cmpct_heap_t *heap, struct free_struct *head, size_t size, size_t rounded_up, int bucket);

#ifdef DEBUG
#define CMPCT_DEBUG
#endif

#define ALLOC_FILL 0x99
#define FREE_FILL 0x77
#define PADDING_FILL 0x55

#define PAGE_SIZE_SHIFT 12
#define PAGE_SIZE (1 << PAGE_SIZE_SHIFT)
#define IS_PAGE_ALIGNED(x) (((uintptr_t)(x) & (PAGE_SIZE - 1)) == 0)
#define PAGES_FOR_BYTES(x) (((x) + PAGE_SIZE - 1) >> PAGE_SIZE_SHIFT)

// Individual allocations above 16kbytes are just fetched directly from the
// block allocator.
#define HEAP_ALLOC_VIRTUAL_BITS 14
// The biggest allocation on a page is limited by size of the biggest bucket.
// With 8 buckets per order of magnitude the biggest bucket is bucker 7 (binary
// 111) and so follows the pattern 1 111 0*.  Bucket sizes don't include the
// header.
#define SMALL_ALLOCATION_LIMIT ((0xf << (HEAP_ALLOC_VIRTUAL_BITS - 4)))
#define ROUNDED_SMALL_ALLOCATION_LIMIT (1 << HEAP_ALLOC_VIRTUAL_BITS)

// Buckets for allocations.  The smallest 15 buckets are 8, 16, 24, etc. up to
// 120 bytes.  After that we round up to the nearest size that can be written
// /^0*1...0*$/, giving 8 buckets per order of binary magnitude.  The freelist
// entries in a given bucket have at least the given size, plus the header
// size.  On 64 bit, the 8 byte bucket is useless, since the freelist header
// is 16 bytes larger than the header, but we have it for simplicity.
#define NUMBER_OF_BUCKETS (1 + 15 + (HEAP_ALLOC_VIRTUAL_BITS - 7) * 8)

// Everything that happens on the heap is 8-byte aligned.
#define NATURAL_ALIGNMENT 8

// All individual memory areas on the heap start with this.
typedef struct header_struct {
    // This is divided up into two 16 bit fields, size and left_size.  We don't use actual
    // 16 bit fields because they don't work in IRAM, which is 32 bit only.
    // left_size: Used to find the previous memory area in address order.
    // size: For the next memory area.  Both size fields include the header.
    size_t size_;
    void *tag;  // Used for the pointer set by the user with heap_caps_set_option.
} header_t;

static INLINE size_t get_left_size(header_t *header)
{
    // The mask here should be 0xffff, but that would cause gcc to emit a 16 bit
    // load. We know the lowest bit of the size (top 16 bits) is always 0 so we
    // just include it here.
    return header->size_ & 0x1ffff;
}

static INLINE void set_left_size(header_t *header, size_t size)
{
    ASSERT(size <= 0xffff);
    header->size_ = (header->size_ & ~0xffff) | size;
}

static INLINE size_t get_size(header_t *header)
{
    size_t field = header->size_;
    // We should just shift down by 16, but that would cause gcc to emit a 16 bit
    // load. We know the highest bit of the left_size (bottom 16 bits) is always 0
    // so we extract that and add it in for no effect.  The expression (field >>
    // 15) & 1 can be obtained with a single extui instruction.  The more obvious
    // (field & 0x8000) would require a load of the 0x8000 constant from the
    // constant pool.
    return (field >> 16) + ((field >> 15) & 1);
}

static INLINE void set_size(header_t *header, size_t size)
{
    ASSERT(size <= 0xffff);
    ASSERT((size & 1) == 0);
    header->size_ = (header->size_ & 0xffff) | (size << 16);
}

typedef struct free_struct {
    header_t header;
    struct free_struct *next;  // Double linked list of free areas in the same bucket.
    struct free_struct *prev;
} free_t;

typedef enum {
    PAGE_FREE = 0,
    PAGE_IN_USE = 1,    // Page is first in an allocation.
    PAGE_CONTINUED = 2  // Page is subsequent in an allocation.
} page_use_t;

// For page allocator, not originally part of cmpctmalloc.  These fields are 32 bit
// so that they work in IRAM on ESP32.
typedef struct Page {
    uint32_t status;
    void *tag;           // Used for the pointer set by the user with heap_caps_set_option.
} Page;

// Allocation arenas are linked together with this header.
typedef struct arena_struct {
    struct arena_struct *previous;
    struct arena_struct *next;
} arena_t;

struct multi_heap_info {
    size_t size;
    size_t remaining;
    size_t free_blocks;
    size_t allocated_blocks;
    void *end_of_heap_structure;
    void *lock;
    void *ignore_free;  // Actually a bool, but use void* so that it works in IRAM.
    free_t *free_lists[NUMBER_OF_BUCKETS];
    // We have some 32 bit words that tell us whether there is an entry in the
    // freelist.
#define BUCKET_WORDS (((NUMBER_OF_BUCKETS) + 31) >> 5)
    uint32_t free_list_bits[BUCKET_WORDS];

    // Doubly linked list for allocation arenas.
    arena_t arenas;

    // For page allocator, not originally part of cmpctmalloc.
    int32_t number_of_pages;
    char *page_base;
    Page pages[1];
};

static ssize_t heap_grow(cmpct_heap_t *heap, free_t **bucket, int pages);

IRAM_ATTR static void lock(cmpct_heap_t *heap)
{
    MULTI_HEAP_LOCK(heap->lock);
}

IRAM_ATTR static void unlock(cmpct_heap_t *heap)
{
    MULTI_HEAP_UNLOCK(heap->lock);
}

static void dump_free(header_t *header)
{
    dprintf(INFO, "\t\tbase %p, end %p, len 0x%zx\n", header, right_header(header), get_size(header));
}

void cmpct_dump(cmpct_heap_t *heap)
{
    lock(heap);
    dprintf(INFO, "Heap dump (using cmpctmalloc):\n");
    dprintf(INFO, "\tsize %lu, remaining %lu, allocated_blocks %lu, free_blocks %lu\n",
            (unsigned long)heap->size,
            (unsigned long)heap->remaining,
            (unsigned long)heap->allocated_blocks,
            (unsigned long)heap->free_blocks);

    dprintf(INFO, "\tfree list:\n");
    for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
        bool header_printed = false;
        free_t *free_area = heap->free_lists[i];
        for (; free_area != NULL; free_area = free_area->next) {
            ASSERT(free_area != free_area->next);
            if (!header_printed) {
                dprintf(INFO, "\tbucket %d\n", i);
                header_printed = true;
            }
            dump_free(&free_area->header);
        }
    }
    unlock(heap);
}

// Operates in sizes that don't include the allocation header.
IRAM_ATTR static int size_to_index_helper(
    size_t size, size_t *rounded_up_out, int adjust, int increment)
{
    // First buckets are simply 8-spaced up to 128.
    if (size <= 128) {
        if (sizeof(size_t) == 8u && size <= sizeof(free_t) - sizeof(header_t)) {
            *rounded_up_out = sizeof(free_t) - sizeof(header_t);
        } else {
            *rounded_up_out = size;
        }
        // No allocation is smaller than 8 bytes, so the first bucket is for 8
        // byte spaces (not including the header).  For 64 bit, the free list
        // struct is 16 bytes larger than the header, so no allocation can be
        // smaller than that (otherwise how to free it), but we have empty 8
        // and 16 byte buckets for simplicity.
        return (size >> 3) - 1;
    }

    // We are going to go up to the next size to round up, but if we hit a
    // bucket size exactly we don't want to go up. By subtracting 8 here, we
    // will do the right thing (the carry propagates up for the round numbers
    // we are interested in).
    size += adjust;
    // After 128 the buckets are logarithmically spaced, every 16 up to 256,
    // every 32 up to 512 etc.  This can be thought of as rows of 8 buckets.
    // We use the compiler intrinsic count-leading-zeros to find the bucket.
    // Eg. 128-255 has 24 leading zeros and we want row to be 4.
    unsigned row = sizeof(size_t) * 8 - (4 + __builtin_clzl(size));
    // For row 4 we want to shift down 4 bits.
    unsigned column = (size >> row) & 7;
    int row_column = (row << 3) | column;
    row_column += increment;
    size = (8 + (row_column & 7)) << (row_column >> 3);
    *rounded_up_out = size;
    // We start with 15 buckets, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96,
    // 104, 112, 120.  Then we have row 4, sizes 128 and up, with the
    // row-column 8 and up.
    unsigned answer = row_column + 15 - 32;
    if (answer >= NUMBER_OF_BUCKETS) FATAL("Invalid free");
    return answer;
}

// Round up size to next bucket when allocating.
IRAM_ATTR static int size_to_index_allocating(size_t size, size_t *rounded_up_out)
{
    size_t rounded = ROUND_UP(size, NATURAL_ALIGNMENT);
    return size_to_index_helper(rounded, rounded_up_out, -8, 1);
}

// Round down size to next bucket when freeing.
IRAM_ATTR static int size_to_index_freeing(size_t size)
{
    size_t dummy;
    return size_to_index_helper(size, &dummy, 0, 0);
}

IRAM_ATTR inline static size_t tag_as_free(size_t left_size)
{
    return left_size | 1;
}

IRAM_ATTR inline static bool is_tagged_as_free(header_t *header)
{
    return (get_left_size(header) & 1) != 0;
}

IRAM_ATTR inline static size_t untag(size_t left_size)
{
    return left_size & ~1;
}

IRAM_ATTR inline static header_t *right_header(header_t *header)
{
    return (header_t *)((char *)header + get_size(header));
}

IRAM_ATTR inline static header_t *left_header(header_t *header)
{
    // We only need to clear the low bit, but we clear the lowest two, because
    // otherwise gcc issues 16 bit operations that fail in IRAM.
    return (header_t *)((char *)header - (get_left_size(header) & ~3));
}

IRAM_ATTR inline static void set_free_list_bit(cmpct_heap_t *heap, int index)
{
    heap->free_list_bits[index >> 5] |= (1u << (31 - (index & 0x1f)));
}

IRAM_ATTR inline static void clear_free_list_bit(cmpct_heap_t *heap, int index)
{
    heap->free_list_bits[index >> 5] &= ~(1u << (31 - (index & 0x1f)));
}

IRAM_ATTR static int find_nonempty_bucket(cmpct_heap_t *heap, int index)
{
    uint32_t mask = (1u << (31 - (index & 0x1f))) - 1;
    mask = mask * 2 + 1;
    mask &= heap->free_list_bits[index >> 5];
    if (mask != 0) return (index & ~0x1f) + __builtin_clz(mask);
    for (index = ROUND_UP(index + 1, 32); index <= NUMBER_OF_BUCKETS; index += 32) {
        mask = heap->free_list_bits[index >> 5];
        if (mask != 0u) return index + __builtin_clz(mask);
    }
    return -1;
}

IRAM_ATTR static bool is_start_of_page_allocation(header_t *header)
{
    return get_left_size(header) == 0;
}

IRAM_ATTR static void create_free_area(cmpct_heap_t *heap, void *address, size_t left_size, size_t size, free_t **bucket)
{
    free_t *free_area = (free_t *)address;
    set_size(&free_area->header, size);
    set_left_size(&free_area->header, tag_as_free(left_size));
    if (bucket == NULL) {
        int index = size_to_index_freeing(size - sizeof(header_t));
        ASSERT(index >= 0);
        set_free_list_bit(heap, index);
        bucket = &heap->free_lists[index];
    }
    free_t *old_head = *bucket;
    if (old_head != NULL) old_head->prev = free_area;
    free_area->next = old_head;
    free_area->prev = NULL;
    *bucket = free_area;
    heap->free_blocks++;
    heap->remaining += size;
#ifdef CMPCT_DEBUG
    memset(free_area + 1, FREE_FILL, size - sizeof(free_t));
#endif
}

IRAM_ATTR static bool is_end_of_page_allocation(header_t *header)
{
    return get_size(header) == 0;
}

// Called with the lock.
IRAM_ATTR static void free_to_page_allocator(cmpct_heap_t *heap, header_t *header, size_t size)
{
    // Arena structure is immediately before the sentinel header.
    arena_t *arena = (arena_t *)header - 1;
    size += sizeof(*arena);

    // Unlink from doubly linked list.
    arena_t *next = arena->next;
    arena_t *previous = arena->previous;
    next->previous = previous;
    previous->next = next;

    DEBUG_ASSERT(IS_PAGE_ALIGNED(size));
    page_free(heap, arena, size >> PAGE_SIZE_SHIFT);
    heap->size -= size;
}

IRAM_ATTR static void fix_left_size(header_t *right, header_t *new_left)
{
    int tag = get_left_size(right) & 1;
    set_left_size(right, (char *)right - (char *)new_left + tag);
}

IRAM_ATTR static void unlink_free(cmpct_heap_t *heap, free_t *free_area, int bucket)
{
    if (get_size(&free_area->header) >= (1 << HEAP_ALLOC_VIRTUAL_BITS)) FATAL("Invalid free");
    heap->remaining -= get_size(&free_area->header);
    heap->free_blocks--;
    ASSERT(heap->remaining < 4000000000u);
    ASSERT(heap->free_blocks < 4000000000u);
    ASSERT(bucket >= 0 && bucket < NUMBER_OF_BUCKETS);
    free_t *next = free_area->next;
    free_t *prev = free_area->prev;
    if (heap->free_lists[bucket] == free_area) {
        heap->free_lists[bucket] = next;
        if (next == NULL) clear_free_list_bit(heap, bucket);
    }
    if (prev != NULL) prev->next = next;
    if (next != NULL) next->prev = prev;
}

IRAM_ATTR static void unlink_free_unknown_bucket(cmpct_heap_t *heap, free_t *free_area)
{
    return unlink_free(heap, free_area, size_to_index_freeing(get_size(&free_area->header) - sizeof(header_t)));
}

// Called with the lock.
IRAM_ATTR static void free_memory(cmpct_heap_t *heap, header_t *header, size_t left_size, size_t size)
{
    create_free_area(heap, header, left_size, size, NULL);
    header_t *left = left_header(header);
    header_t *right = right_header(header);
    fix_left_size(right, header);
    // The alignment test is both for efficiency and to ensure that non-page
    // aligned areas that we give to the allocator are not returned to the page
    // allocator, which cannot handle them.
    if (IS_PAGE_ALIGNED((uintptr_t)left - sizeof(arena_t)) &&
        IS_PAGE_ALIGNED((uintptr_t)right + sizeof(header_t)) &&
        is_start_of_page_allocation(left) &&
        is_end_of_page_allocation(right)) {
        // The entire page was free and can be returned to the page allocator.
        unlink_free_unknown_bucket(heap, (free_t *)header);
        free_to_page_allocator(heap, left, size + get_size(left) + sizeof(header_t));
    }
}

IRAM_ATTR static void *create_allocation_header(
    header_t *header, size_t size, size_t left_size, void *tag)
{
    set_left_size(header, untag(left_size));
    set_size(header, size);
    header->tag = tag;
    return header + 1;
}

#ifdef TEST_CMPCTMALLOC

void cmpct_test_buckets(void)
{
    size_t rounded;
    int bucket; // Check for the 8-spaced buckets up to 128.
    for (unsigned i = 0; i <= 128; i++) {
        // Round up when allocating.
        bucket = size_to_index_allocating(i, &rounded);
        ASSERT(bucket >= 0 && bucket < NUMBER_OF_BUCKETS);
        unsigned expected = (ROUND_UP(i, 8) >> 3) - 1;
        USE(expected);
        USE(bucket);
        ASSERT(bucket == expected);
        ASSERT(IS_ALIGNED(rounded, 8));
        ASSERT(rounded >= i);
        if (i >= sizeof(free_t) - sizeof(header_t)) {
            // Once we get above the size of the free area struct (4 words), we
            // won't round up much for these small size.
            ASSERT(rounded - i < 8);
        }
        // Only rounded sizes are freed.
        if ((i & 7) == 0) {
            // Up to size 128 we have exact buckets for each multiple of 8.
            ASSERT(bucket == (unsigned)size_to_index_freeing(i));
        }
    }
    int bucket_base = 7;
    for (unsigned j = 16; j < 1024; j *= 2, bucket_base += 8) {
        // Note the "<=", which ensures that we test the powers of 2 twice to ensure
        // that both ways of calculating the bucket number match.
        for (unsigned i = j * 8; i <= j * 16; i++) {
            // Round up to j multiple in this range when allocating.
            bucket = size_to_index_allocating(i, &rounded);
            ASSERT(bucket >= 0 && bucket < NUMBER_OF_BUCKETS);
            unsigned expected = bucket_base + ROUND_UP(i, j) / j;
            USE(expected);
            ASSERT(bucket == expected);
            ASSERT(IS_ALIGNED(rounded, j));
            ASSERT(rounded >= i);
            ASSERT(rounded - i < j);
            // Only 8-rounded sizes are freed or chopped off the end of a free area
            // when allocating.
            if ((i & 7) == 0) {
                // When freeing, if we don't hit the size of the bucket precisely,
                // we have to put the free space into a smaller bucket, because
                // the buckets have entries that will always be big enough for
                // the corresponding allocation size (so we don't have to
                // traverse the free chains to find a big enough one).
                if ((i % j) == 0) {
                    ASSERT((int)bucket == size_to_index_freeing(i));
                } else {
                    ASSERT((int)bucket - 1 == size_to_index_freeing(i));
                }
            }
        }
    }
}

static void cmpct_test_get_back_newly_freed_helper(cmpct_heap_t *heap, size_t size)
{
    void *allocated = cmpct_alloc(heap, size);
    if (allocated == NULL) return;
    char *allocated2 = cmpct_alloc(heap, 8);
    char *expected_position = (char *)allocated + size;
    if (allocated2 < expected_position || allocated2 > expected_position + 128) {
        // If the allocated2 allocation is not in the same OS allocation as the
        // first allocation then the test may not work as expected (the memory
        // may be returned to the OS when we free the first allocation, and we
        // might not get it back).
        cmpct_free(heap, allocated);
        cmpct_free(heap, allocated2);
        return;
    }

    cmpct_free(heap, allocated);
    void *allocated3 = cmpct_alloc(heap, size);
    // To avoid churn and fragmentation we would want to get the newly freed
    // memory back again when we allocate the same size shortly after.
    ASSERT(allocated3 == allocated);
    cmpct_free(heap, allocated2);
    cmpct_free(heap, allocated3);
}

static uint64_t test_random_state[2] = {0x12837219, 0x29834};

static uint64_t cmpct_test_random_next()
{
    uint64_t s1 = test_random_state[0];
    const uint64_t s0 = test_random_state[1];
    const uint64_t result = s0 + s1;
    test_random_state[0] = s0;
    s1 ^= s1 << 23;
    test_random_state[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
    return result;
}

static void cmpct_test_get_back_newly_freed(cmpct_heap_t *heap)
{
    size_t increment = 16;
    for (size_t i = 128; i < PAGE_SIZE; i *= 2, increment *= 2) {
        for (size_t j = i; j < i * 2; j += increment) {
            if (i - 1 <= SMALL_ALLOCATION_LIMIT) {
                cmpct_test_get_back_newly_freed_helper(heap, i - 1);
            }
            if (i <= SMALL_ALLOCATION_LIMIT) {
                cmpct_test_get_back_newly_freed_helper(heap, i);
            }
            if (i + 1 <= SMALL_ALLOCATION_LIMIT) {
                cmpct_test_get_back_newly_freed_helper(heap, i + 1);
            }
        }
    }
    for (size_t i = ROUNDED_SMALL_ALLOCATION_LIMIT / 2; i <= ROUNDED_SMALL_ALLOCATION_LIMIT; i++) {
        if (i <= SMALL_ALLOCATION_LIMIT) {
            cmpct_test_get_back_newly_freed_helper(heap, i);
        }
    }
}

typedef struct cmpct_test_visit_record_struct {
    bool visited;
    void *address;
    size_t largest_free_area_visited;
    size_t largest_overhead_area_visited;
    size_t size;
} cmpct_test_visit_record;

static bool cmpct_test_visitor_keep(void *r, void *tag, void *address, size_t size)
{
    cmpct_test_visit_record *record = (cmpct_test_visit_record *)r;
    if (tag == (void *)CMPCTMALLOC_ITERATE_TAG_FREE) {
        if (size > record->largest_free_area_visited) record->largest_free_area_visited = size;
    } else if (tag == (void *)CMPCTMALLOC_ITERATE_TAG_HEAP_OVERHEAD) {
        if (size > record->largest_overhead_area_visited) record->largest_overhead_area_visited = size;
    } else if (tag != NULL) {
        record->visited = true;
        record->address = address;
        record->size = size;
    }
    return false;  // Don't free.
}

static bool cmpct_test_visitor_free(void *r, void *tag, void *address, size_t size)
{
    cmpct_test_visit_record *record = (cmpct_test_visit_record *)r;
    record->visited = true;
    record->address = address;
    record->size = size;
    return true;  // Free the allocation.
}

static void cmpct_test_tagged_allocations(cmpct_heap_t *heap)
{
    cmpct_set_option(MALLOC_OPTION_THREAD_TAG, NULL);  // From here, allocations are not tagged.
    void *alloc_1 = cmpct_alloc(heap, 113);
    void *alloc_2 = cmpct_alloc(heap, 113);
    cmpct_test_visit_record record;
    record.visited = false;

    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(!record.visited);  // No allocations were tagged.

    cmpct_set_option(MALLOC_OPTION_THREAD_TAG, &record);  // From here, allocations are tagged.

    record.largest_free_area_visited = 0;
    record.largest_overhead_area_visited = 0;

    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(!record.visited);  // Still no tagged allocations.
    ASSERT(record.largest_free_area_visited == 0);
    ASSERT(record.largest_overhead_area_visited == 0);

    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, CMPCTMALLOC_ITERATE_UNUSED);
    ASSERT(!record.visited);  // Still no tagged allocations.
    ASSERT(record.largest_free_area_visited > 100);
    ASSERT(record.largest_overhead_area_visited > 100);

    void *alloc_3 = cmpct_alloc(heap, 113);  // This allocation will be tagged.
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(record.visited);  // We found the tagged allocation.
    ASSERT(record.address == alloc_3);
    ASSERT(record.size == 120);  // 113 rounded up.

    record.visited = false;
    record.address = NULL;
    record.size = 0;

    // Use flag so all allocations are iterated over, but the callback ignores those
    // with a null tag.
    cmpct_iterate_tagged_memory_areas(heap, &record, NULL, cmpct_test_visitor_keep, CMPCTMALLOC_ITERATE_ALL_ALLOCATIONS);
    ASSERT(record.visited);  // We found the tagged allocation.
    ASSERT(record.address == alloc_3);
    ASSERT(record.size >= 113);

    record.visited = false;
    record.address = NULL;
    record.size = 0;

    cmpct_free(heap, alloc_3);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(!record.visited);  // The allocation was freed, so it is not found.

    cmpct_free(heap, alloc_1);
    cmpct_free(heap, alloc_2);

    alloc_1 = cmpct_malloc_impl(heap, PAGE_SIZE * 2);  // New allocation more than one page
    record.visited = false;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(record.visited);  // It was found.
    ASSERT(record.size == PAGE_SIZE * 2);

    record.visited = false;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_keep, 0);
    ASSERT(record.visited);  // Still found (not freed by the keep callback).

    cmpct_free_impl(heap, alloc_1);

    alloc_1 = cmpct_alloc(heap, 113);  // Create new tagged allocation.

    record.visited = false;
    record.address = NULL;
    record.size = 0;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free, 0);  // Use freeing callback.
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_1);
    ASSERT(record.size >= 113);

    record.visited = false;
    record.address = NULL;
    record.size = 0;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free, 0);
    ASSERT(!record.visited);  // It was freed last time so it's gone now.

    // Create big tagged allocations that are put on the page heap.
    alloc_1 = cmpct_malloc_impl(heap, PAGE_SIZE);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free, 0);
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_1);
    ASSERT(record.size == PAGE_SIZE);

    alloc_2 = cmpct_malloc_impl(heap, ROUNDED_SMALL_ALLOCATION_LIMIT * 3 / 2);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free, 0);
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_2);
    ASSERT(record.size == (ROUNDED_SMALL_ALLOCATION_LIMIT * 3) / 2);
}

static void cmpct_test_churn(cmpct_heap_t *heap)
{
    size_t remaining = cmpct_free_size_impl(heap);
    size_t heap_size = heap->size;
    USE(remaining);
    USE(heap_size);
#define TEST_ITERATIONS 4096
    char *allocations[TEST_ITERATIONS] = { NULL };
    for (int i = 0; i < TEST_ITERATIONS; i++) {
        int free_index = (i + 500) % TEST_ITERATIONS;
        cmpct_free(heap, allocations[free_index]);
        allocations[free_index] = NULL;
        size_t size = cmpct_test_random_next() & 0xff;
        allocations[i] = cmpct_alloc(heap, size);
        ASSERT(cmpct_get_allocated_size_impl(heap, allocations[i]) >= size);
        // Waste is rather more on 64 bit because the doubly-linked freelist
        // entries are so big.
        if (sizeof(void *) == 4) {
            ASSERT(cmpct_get_allocated_size_impl(heap, allocations[i]) <= size * 1.06 + sizeof(free_t));
        }
        for (size_t j = 0; j < size; j++) {
            allocations[i][j] = cmpct_test_random_next();
        }
    }
    for (int i = 0; i < TEST_ITERATIONS; i++) {
        cmpct_free(heap, allocations[i]);
    }
    ASSERT(remaining == cmpct_free_size_impl(heap));
    ASSERT(heap_size == heap->size);
}

#endif  // TEST_CMPCTMALLOC

IRAM_ATTR static int get_bucket_for_size(cmpct_heap_t *heap, size_t size, int start_bucket)
{
    int bucket = find_nonempty_bucket(heap, start_bucket);
    if (bucket == -1) {
        // Grow heap by a few pages. If we can.
        int pages_needed = ROUND_UP(size + ROUNDED_SMALL_ALLOCATION_LIMIT - SMALL_ALLOCATION_LIMIT, PAGE_SIZE) >> PAGE_SIZE_SHIFT;
        if (heap_grow(heap, NULL, pages_needed) < 0) {
            unlock(heap);
            return -1;
        }
        bucket = find_nonempty_bucket(heap, start_bucket);
        // Allocation is always less than one page so this must succeed.
        ASSERT(bucket >= 0 && bucket < NUMBER_OF_BUCKETS);
    }
    return bucket;
}

IRAM_ATTR void *cmpct_alloc(cmpct_heap_t *heap, size_t size)
{
    // In C++ we are not allowed to return null for zero length allocations, so
    // bump the size and return a small allocation instead.
    if (size == 0u) size = 1;

    ASSERT(size <= SMALL_ALLOCATION_LIMIT);

    size_t rounded_up;
    int start_bucket = size_to_index_allocating(size, &rounded_up);

    rounded_up += sizeof(header_t);

    lock(heap);

    int bucket = get_bucket_for_size(heap, size, start_bucket);
    if (bucket == -1) return NULL;

    free_t *head = heap->free_lists[bucket];
    return allocation_tail(heap, head, size, rounded_up, bucket);
}

// Takes a block on the free list, unlinks it, possibly creates a new freelist
// entry from the excess, and returns the newly allocated memory.  On entry the
// heap should be locked.  Unlocks the heap.
IRAM_ATTR static void *allocation_tail(cmpct_heap_t *heap, free_t *head, size_t size, size_t rounded_up, int bucket)
{
    header_t *block = &head->header;
    size_t block_size = get_size(block);
    size_t rest = block_size - rounded_up;
    // We can't carve off the rest for a new free space if it's smaller than
    // the free-list linked structure.  We also don't carve it off if it's less
    // than 3.2% the size of the allocation.  This is to avoid small long-lived
    // allocations being placed right next to large allocations, hindering
    // coalescing and returning pages to the OS.  Note that the buckets already
    // cause allocations to be rounded up to the nearest bucket size.  The
    // buckets are spaced in intervals that are between 6% and 12% apart.
    if (rest >= sizeof(free_t) && rest > (size >> 5)) {
        header_t *right = right_header(block);
        unlink_free(heap, head, bucket);
        header_t *free = (header_t *)((char *)head + rounded_up);
        create_free_area(heap, free, rounded_up, rest, NULL);
        fix_left_size(right, free);
        block_size -= rest;
    } else {
        unlink_free(heap, head, bucket);
    }
    void *tag = GET_THREAD_LOCAL_TAG;
    void *result =
        create_allocation_header(block, block_size, get_left_size(block), tag);
    heap->allocated_blocks++;
    for (int i = 0; i < rounded_up - sizeof(header_t); i += sizeof(int)) {
        ((int *)(result))[i >> 2] = 0;
    }
#ifdef CMPCT_DEBUG
    memset(result, ALLOC_FILL, size);
    memset(((char *)result) + size, PADDING_FILL, (rounded_up - size) - sizeof(header_t));
#endif
    unlock(heap);
    return result;
}

IRAM_ATTR void cmpct_free_optionally_locked(cmpct_heap_t *heap, void *payload, bool use_locking)
{
    if (payload == NULL) return;
    if (heap->ignore_free) return;
    header_t *header = (header_t *)payload - 1;
    if (is_tagged_as_free(header)) FATAL("Double free");
    size_t size = get_size(header);
    if (use_locking) lock(heap);
    heap->allocated_blocks--;
    header_t *left = left_header(header);
    header_t *right = right_header(header);
    if (is_tagged_as_free(left)) {
        // Place a free marker in the middle of the coalesced free area in
        // order to catch more double frees.
        set_left_size(header, tag_as_free(get_left_size(header)));
        // Coalesce with left free object.
        unlink_free_unknown_bucket(heap, (free_t *)left);
        if (is_tagged_as_free(right)) {
            // Coalesce both sides.
            unlink_free_unknown_bucket(heap, (free_t *)right);
            free_memory(heap, left, get_left_size(left), get_size(left) + size + get_size(right));
        } else {
            // Coalesce only left.
            free_memory(heap, left, get_left_size(left), get_size(left) + size);
        }
    } else {
        if (is_tagged_as_free(right)) {
            // Coalesce only right.
            unlink_free_unknown_bucket(heap, (free_t *)right);
            free_memory(heap, header, get_left_size(header), size + get_size(right));
        } else {
            free_memory(heap, header, get_left_size(header), size);
        }
    }
    if (use_locking) unlock(heap);
}

IRAM_ATTR void cmpct_free(cmpct_heap_t *heap, void *payload)
{
    cmpct_free_optionally_locked(heap, payload, true);
}

INLINE void cmpct_free_already_locked(cmpct_heap_t *heap, void *payload)
{
    cmpct_free_optionally_locked(heap, payload, false);
}

// Get the rounded-up size of an allocation on the cmpct heap, given the
// address.  Can be called without the lock.
IRAM_ATTR static size_t allocation_size(void *payload)
{
    header_t *header = (header_t *)payload - 1;
    size_t size = get_size(header) - sizeof(header_t);
    return size;
}

IRAM_ATTR void *cmpct_realloc(cmpct_heap_t *heap, void *payload, size_t size)
{
    if (payload == NULL) return cmpct_alloc(heap, size);
    size_t old_size = allocation_size(payload);
    void *new_payload = cmpct_alloc(heap, size);
    memcpy(new_payload, payload, MIN(size, old_size));
    cmpct_free(heap, payload);
    return new_payload;
}

// Each allocation area has an arena structure at the start to link them
// together and a sentinel at either end that is the size of one header.
static const size_t arena_overhead = 2 * sizeof(header_t) + sizeof(arena_t);

IRAM_ATTR static void add_to_heap(cmpct_heap_t *heap, void *new_area, size_t size, free_t **bucket)
{
    heap->size += size;
    void *top = (char *)new_area + size;
    arena_t *new_arena = (arena_t *)new_area;
    // Link into doubly linked list.
    arena_t *old_next = heap->arenas.next;
    new_arena->next = old_next;
    new_arena->previous = &heap->arenas;
    heap->arenas.next = new_arena;
    old_next->previous = new_arena;

    header_t *left_sentinel = (header_t *)(new_arena + 1);
    // Not free, stops attempts to coalesce left.
    create_allocation_header(left_sentinel, sizeof(header_t), 0, NULL);
    header_t *new_header = left_sentinel + 1;
    size_t free_size = size - arena_overhead;
    create_free_area(heap, new_header, sizeof(header_t), free_size, bucket);
    header_t *right_sentinel = (header_t *)(top - sizeof(header_t));
    // Not free, stops attempts to coalesce right.
    create_allocation_header(right_sentinel, 0, free_size, NULL);
}

// Grab n pages of memory from the page allocator.
// Called with the lock, apart from during init.
IRAM_ATTR static ssize_t heap_grow(cmpct_heap_t *heap, free_t **bucket, int pages)
{
    // Allocate one page more.  The allocation tag is a pointer to the heap
    // itself so that it won't match any allocation tag used by the program.
    void *ptr = page_alloc(heap, pages, heap);
    if (ptr == NULL) return -1;
    LTRACEF("growing heap by 0x%x bytes, new ptr %p\n", pages << PAGE_SIZE_SHIFT, ptr);
    add_to_heap(heap, ptr, pages * PAGE_SIZE, bucket);
    return pages * PAGE_SIZE;
}

void cmpct_init(cmpct_heap_t *heap)
{
    LTRACE_ENTRY;

    // Initialize the free list.
    for (int i = 0; i < NUMBER_OF_BUCKETS; i++) {
        heap->free_lists[i] = NULL;
    }
    for (int i = 0; i < BUCKET_WORDS; i++) {
        heap->free_list_bits[i] = 0;
    }

    heap->lock = NULL;
    heap->ignore_free = NULL;
    heap->remaining = 0;
    heap->size = 0;
    heap->free_blocks = 0;
    heap->allocated_blocks = 0;
    // Empty doubly linked list points to itself.
    heap->arenas.previous = &heap->arenas;
    heap->arenas.next = &heap->arenas;
    heap->end_of_heap_structure = NULL;
}

// Takes a memory area for a heap.  The first part of the memory that was given
// is used for an instance of cmpct_heap_t with the bookkeeping information for
// the heap.  The whole pages are added to the page allocator for this heap.
// Space that is not page-aligned is put on the freelist.  It will never be
// returned to the page allocator because only page aligned free entries trigger
// the code that detects wholly empty pages and returns them to the page allocator.
cmpct_heap_t *cmpct_register_impl(void *start, size_t size)
{
    if (size < sizeof(cmpct_heap_t)) return NULL;  // Area too small.
    // We can't have more pages than the rounded down size.
    intptr_t pages = size >> PAGE_SIZE_SHIFT;
    ASSERT(pages >= 0);

    uintptr_t start_int = (uintptr_t)start;
    intptr_t header_waste = ROUND_UP(start_int, sizeof(header_t)) - start_int;
    uintptr_t end_int = (uintptr_t)start + size;
    start_int += header_waste;
    uintptr_t end_of_struct = start_int + sizeof(cmpct_heap_t) + pages * sizeof(Page);
    ASSERT(end_of_struct <= end_int);
    uintptr_t start_of_first_page = ROUND_UP(end_of_struct, PAGE_SIZE);

    // May be a little smaller than the old value of pages.
    pages = start_of_first_page > end_int ? 0 : (end_int - start_of_first_page) >> PAGE_SIZE_SHIFT;

    cmpct_heap_t *page_heap = (cmpct_heap_t *)start_int;
    page_heap->number_of_pages = pages;
    page_heap->page_base = (char *)start_of_first_page;
    for (size_t i = 0; i < pages; i++) {
        page_heap->pages[i].status = PAGE_FREE;
        page_heap->pages[i].tag = NULL;
    }
    // A sentinel page is in_use, but not a continuation of any previous
    // allocation.  This is just in the index, we don't actually waste a page
    // on this.
    page_heap->pages[pages].status = PAGE_IN_USE;
    page_heap->pages[pages].tag = NULL;

    cmpct_init(page_heap);

    uintptr_t rest_of_zeroth_page = ROUND_UP(end_of_struct, sizeof(header_t));

    // If we were handed a very small amount of memory then we just give the
    // entire space to the small allocation arena.
    if (start_of_first_page >= end_int) {
        intptr_t rest = ROUND_DOWN(end_int, sizeof(header_t)) - rest_of_zeroth_page;
        if (rest >= (intptr_t)(arena_overhead + sizeof(free_t))) {
            add_to_heap(page_heap, (void *)rest_of_zeroth_page, rest, NULL);
        }
    } else {
        // Unaligned memory before the start of the first page is added to the
        // heap for small allocations.
        intptr_t rest = start_of_first_page - rest_of_zeroth_page;
        if (rest > (intptr_t)(arena_overhead + sizeof(free_t))) {
            add_to_heap(page_heap, (void *)rest_of_zeroth_page, rest, NULL);
            page_heap->end_of_heap_structure = (void *)rest_of_zeroth_page;
        }
        // Unaligned memory after the end of the last page can also be added to
        // the heap for small allocations.
        size_t end_of_last_page = start_of_first_page + PAGE_SIZE * pages;
        rest = ROUND_DOWN(end_int, sizeof(header_t)) - end_of_last_page;
        if (rest > (intptr_t)(arena_overhead + sizeof(free_t))) {
            add_to_heap(page_heap, (void *)end_of_last_page, rest, NULL);
        }
    }
    return page_heap;
}

IRAM_ATTR void *cmpct_malloc_impl(cmpct_heap_t *heap, size_t size)
{
    // Allocations of page-aligned sizes go directly to the page allocator,
    // but zero-length allocations can't be handled by the page allocator.
    // Also, large allocations are handled by the page allocator.
    if (size <= SMALL_ALLOCATION_LIMIT &&
        (size == 0 || (size & (PAGE_SIZE - 1)) != 0)) {
        // The SMALL_ALLOCATION_LIMIT is determined by the biggest bucket, but
        // we also need to ensure the largest possible in-page space is large
        // enough.  A page has a given overhead, and a single almost-page-filling
        // allocation also needs a header.
        return cmpct_alloc(heap, size);
    }
    lock(heap);
    void *tag = GET_THREAD_LOCAL_TAG;
    void *result = page_alloc(heap, PAGES_FOR_BYTES(size), PAGE_SIZE, tag);
    unlock(heap);
    return result;
}

IRAM_ATTR void *cmpct_aligned_alloc_impl(cmpct_heap_t *heap, size_t size, size_t alignment) {
    // Only allow powers of 2 as alignments.
    if (((alignment - 1) & alignment) != 0) return NULL;

    // The page allocator already has the ability to return allocations that
    // are more aligned than the page size.
    if (alignment >= PAGE_SIZE / 2) {
        // We take 2k (half-page) allocations in here too, because treating them as
        // page allocations will waste 2k, but putting them in the normal system
        // actually wastes even more.
        lock(heap);
        void *tag = GET_THREAD_LOCAL_TAG;
        void *result = page_alloc(heap, PAGES_FOR_BYTES(size), alignment, tag);
        unlock(heap);
        return result;
    }

    if (alignment <= NATURAL_ALIGNMENT) return cmpct_alloc(heap, size);

    size = ROUND_UP(size, NATURAL_ALIGNMENT);

    // Our approach to aligned allocations requires us to temporarily create a
    // free space of the required size, so there's a minimum size below which
    // it doesn't work.
    if (size < sizeof(free_t)) size = sizeof(free_t);

    // This gives us at least one alignment of slack to position the returned
    // pointer, plus space for the header.  Worst case looking from the back of
    // the allocation is that there almost an alignment-worth of waste at the
    // end, the allocation requested, then an allocation header,
    // sizeof(header_t), then a free list entry, sizeof(free_t).
    size_t aligned_size = size + alignment - NATURAL_ALIGNMENT + sizeof(header_t) + sizeof(free_t);

    size_t dummy;
    int start_bucket = size_to_index_allocating(aligned_size, &dummy);

    lock(heap);

    int bucket = get_bucket_for_size(heap, aligned_size, start_bucket);
    if (bucket == -1) return NULL;  // Out of memory.

    free_t *head = heap->free_lists[bucket];
    header_t *block = &head->header;
    uintptr_t first_possible_location = (uintptr_t)(block + 1);
    uintptr_t location = ROUND_UP(first_possible_location, alignment);
    size_t size_with_header = size + sizeof(header_t);
    if (location == first_possible_location) {
        // Luckily already aligned.
        return allocation_tail(heap, head, size, size_with_header, bucket);
    }
    while (location - first_possible_location < sizeof(free_t)) {
        // No space for the free list header.
        location += alignment;
    }
    // We are splitting a free block into an unneeded part on the left and an
    // aligned part.
    header_t *right = right_header(block);
    unlink_free(heap, head, bucket);
    size_t unneeded_free_size = location - first_possible_location;
    size_t aligned_part_size = get_size(block) - unneeded_free_size;
    create_free_area(heap, head, get_left_size(block), unneeded_free_size, NULL);
    header_t *aligned_header = (header_t *)location - 1;
    // Note: This is the only moment where there are two free areas adjacent to
    // each other.  Normally we coalesce agressively.
    create_free_area(heap, aligned_header, unneeded_free_size, aligned_part_size, NULL);
    fix_left_size(right, aligned_header);

    // Create the allocation from the aligned area, possibly freeing the excess
    // on the right.
    return allocation_tail(heap, (free_t *)aligned_header, size, size_with_header, size_to_index_freeing(aligned_part_size - sizeof(header_t)));
}

IRAM_ATTR static bool is_page_allocated(cmpct_heap_t *heap, void *p)
{
    if (p == NULL || ((size_t)p & (PAGE_SIZE - 1)) != 0) return false;
    // The pointer is page-aligned, so it might be a page-allocation, or it
    // could just be a normal allocation in the middle of a multi-page arena
    // that happens to be aligned.
    size_t page = page_number(heap, p);
    // Only the first page in a multi-page allocation is marked as PAGE_IN_USE.
    // The others are marked as PAGE_CONTINUED.  This also applies to multiple
    // pages that were taken from the page allocator for use in a multi-page
    // arena.
    return heap->pages[page].status == PAGE_IN_USE;
}

IRAM_ATTR void cmpct_free_impl(cmpct_heap_t *heap, void *p)
{
    if (is_page_allocated(heap, p)) {
        lock(heap);
        page_free(heap, p, 0);
        unlock(heap);
    } else {
        cmpct_free(heap, p);
    }
}

// Get the page number of a page-aligned pointer in the current heap.  Called
// with the lock.
IRAM_ATTR static size_t page_number(cmpct_heap_t *heap, void *p)
{
    size_t offset = (char *)p - heap->page_base;
    size_t page = offset >> PAGE_SIZE_SHIFT;
    ASSERT(heap->pages[page].status == PAGE_IN_USE);
    return page;
}

IRAM_ATTR size_t cmpct_get_allocated_size_impl(cmpct_heap_t *heap, void *p)
{
    if (p == NULL) return 0;
    if (!is_page_allocated(heap, p)) {
        size_t size = allocation_size(p);
        return size;
    }
    size_t page = page_number(heap, p);
    for (size_t i = 1; true; i++) {
        if (heap->pages[page + i].status != PAGE_CONTINUED) return i << PAGE_SIZE_SHIFT;
    }
}

// This is a very simple version of realloc, which always creates a new
// area and copies to it.
IRAM_ATTR void *cmpct_realloc_impl(cmpct_heap_t *heap, void *p, size_t size)
{
    if (!size) {
        cmpct_free_impl(heap, p);
        return NULL;
    }
    void *new_allocation = cmpct_malloc_impl(heap, size);
    if (!p || !new_allocation) return new_allocation;
    size_t old_size = cmpct_get_allocated_size_impl(heap, p);
    memcpy(new_allocation, p, MIN(old_size, size));
    cmpct_free_impl(heap, p);
    return new_allocation;
}

size_t cmpct_free_size_impl(cmpct_heap_t *heap)
{
    return heap->remaining;
}

void cmpct_get_info_impl(cmpct_heap_t *heap, multi_heap_info_t *info)
{
    lock(heap);

    info->total_free_bytes = heap->remaining;
    // Subtract the two end sentinels and the free list header that are always
    // the minimum that is taken by the heap.
    info->total_allocated_bytes = (heap->size - heap->remaining) - 3 * sizeof(header_t);
    info->largest_free_block = 0;
    // TODO: We don't currently keep track of the all-time low number of free
    // bytes.
    info->minimum_free_bytes = 0;
    info->allocated_blocks = heap->allocated_blocks;
    info->free_blocks = heap->free_blocks;
    size_t current_page_run = 0;
    page_use_t current_status = PAGE_FREE;
    void *current_tag = NULL;
    // Include sentinel in iteration.
    for (size_t i = 0; i <= heap->number_of_pages; i++) {
        if (heap->pages[i].status == current_status && i != heap->number_of_pages) {
            current_page_run += PAGE_SIZE;
        } else {
            if (current_status == PAGE_FREE) {
                info->total_free_bytes += current_page_run;
                if (current_page_run != 0) {
                    info->free_blocks++;
                    if (current_page_run > info->largest_free_block) {
                        info->largest_free_block = current_page_run;
                    }
                }
            } else {
                if (current_tag != heap) {  // Pages used for the sub-page allocator are self-tagged.
                    info->total_allocated_bytes += current_page_run;
                    if (current_page_run != 0) info->allocated_blocks++;
                }
            }
            if (heap->pages[i].status == PAGE_FREE) {
                current_status = PAGE_FREE;
            } else {
                current_status = PAGE_CONTINUED;
                current_tag = heap->pages[i].tag;
            }
            current_page_run = PAGE_SIZE;
        }
    }
    if (info->largest_free_block == 0) {
        // All pages are taken so largest free block is in the cmpctmalloc-
        // controlled area.
        for (int i = BUCKET_WORDS * 32 - 1; i >= 0; i--) {
            if (find_nonempty_bucket(heap, i) != -1) {
                free_t *head = heap->free_lists[i];
                size_t size = get_size(&head->header) - sizeof(header_t);
                if (size <= 128) {
                    // These buckets are precise.
                    info->largest_free_block = size;
                } else {
                    // For larger sizes the bucket sizes are approximate, so
                    // round down by 1/8th to get a size we are guaranteed to
                    // be able to deliver.
                    info->largest_free_block = (size_t)(size * 0.87499) & ~7l;
                }
                break;
            }
        }
    }
    info->total_blocks = info->free_blocks + info->allocated_blocks;
    unlock(heap);
}

size_t cmpct_minimum_free_size_impl(cmpct_heap_t *heap)
{
    multi_heap_info_t info;
    cmpct_get_info_impl(heap, &info);
    return info.minimum_free_bytes;
}

// Called with the lock.
IRAM_ATTR static void *page_alloc(cmpct_heap_t *heap, intptr_t pages, void *tag)
{
    for (int i = 0; i <= heap->number_of_pages - pages; i++) {
        if (heap->pages[i].status == PAGE_FREE) {
            bool big_enough = true;
            for (int j = 1; j < pages; j++) {
                if (heap->pages[i + j].status != PAGE_FREE) {
                    big_enough = false;
                    i += j;
                    break;
                }
            }
            if (big_enough) {
                heap->pages[i].status = PAGE_IN_USE;
                heap->pages[i].tag = tag;
                for (int j = 1; j < pages; j++) {
                    heap->pages[i + j].status = PAGE_CONTINUED;
                }
                void *result = heap->page_base + i * PAGE_SIZE;
                for (int i = 0; i < pages << PAGE_SIZE_SHIFT; i += sizeof(int)) {
                    ((int *)(result))[i >> 2] = 0;
                }
                return heap->page_base + i * PAGE_SIZE;
            }
        }
    }
    return NULL;
}

IRAM_ATTR static void page_iterate(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback, int flags)
{
    for (int i = 0; i < heap->number_of_pages; i++) {
        int status = heap->pages[i].status;
        if (status == PAGE_IN_USE || status == PAGE_FREE) {
            // A flag can indicate that we should iterate over all allocations, but we still
            // don't iterate over the page allocations that the sub-page allocator made.
            bool iterate_free = false;
            bool iterate_allocated = false;
            int continuation_status = 0;
            void *found_tag = NULL;
            if (status == PAGE_FREE && (flags & CMPCTMALLOC_ITERATE_UNUSED) != 0) {
                iterate_free = true;
                continuation_status = PAGE_FREE;  // It's all one area as long as we see free pages.
                found_tag = (void *)CMPCTMALLOC_ITERATE_TAG_FREE;
            }
            if (status == PAGE_IN_USE &&
                (heap->pages[i].tag == tag ||
                 ((flags & CMPCTMALLOC_ITERATE_ALL_ALLOCATIONS) != 0 && heap->pages[i].tag != heap))) {
                iterate_allocated = true;
                continuation_status = PAGE_CONTINUED;  // It's all one area as long as we see continued pages.
                found_tag = heap->pages[i].tag;
            }
            if (iterate_free || iterate_allocated) {
                for (int j = 1; true; j++) {
                    ASSERT(i + j <= heap->number_of_pages);
                    if (heap->pages[i + j].status != continuation_status) {
                        void *allocation = heap->page_base + i * PAGE_SIZE;
                        if (callback(user_data, found_tag, allocation, j * PAGE_SIZE) && iterate_allocated) {
                            // Callback indicates we should free the memory.
                            page_free(heap, allocation, j);
                        }
                        i += j - 1;
                        break;
                    }
                }
            }
        }
    }
}

// Frees a number of pages allocated in one chunk.  This version of cmpctmalloc
// does not contain support for trimming a region obtained from the page
// allocator, so the number of pages is always the number of pages allocated,
// and we ignore the page count argument.  Called with the lock.
IRAM_ATTR static void page_free(cmpct_heap_t *heap, void *address, int page_count_dummy)
{
    size_t page = page_number(heap, address);
    if (page >= heap->number_of_pages || heap->pages[page].status != PAGE_IN_USE) {
        FATAL("Invalid free");
    }
    for (intptr_t j = page + 1; heap->pages[j].status == PAGE_CONTINUED; j++) {
        heap->pages[j].status = PAGE_FREE;
    }
    heap->pages[page].status = PAGE_FREE;
}

void multi_heap_set_lock(cmpct_heap_t *heap, void *lock)
{
    heap->lock = lock;
}

void cmpct_set_option(cmpct_heap_t *heap, int option, void *value)
{
    if (option == MALLOC_OPTION_THREAD_TAG) {
#if !defined(TEST_CMPCTMALLOC) && !defined(CMPCTMALLOC_ON_LINUX)
        first_allocations = false;
        assert(MULTI_HEAP_THREAD_TAG_INDEX < configNUM_THREAD_LOCAL_STORAGE_POINTERS);
        vTaskSetThreadLocalStoragePointer(NULL, MULTI_HEAP_THREAD_TAG_INDEX, value);
#else
        pthread_setspecific(tls_key, value);
#endif
    } else if (option == MALLOC_OPTION_DISABLE_FREE) {
        heap->ignore_free = value;
    }
}

void cmpct_iterate_tagged_memory_areas(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback, int flags)
{
    if ((flags & CMPCTMALLOC_ITERATE_UNLOCKED) == 0) {
        lock(heap);
    }
    bool iterate_heap_structure = (flags & CMPCTMALLOC_ITERATE_UNUSED) != 0;
    page_iterate(heap, user_data, tag, callback, flags);
    arena_t *end = &heap->arenas;
    header_t *to_free = NULL;
    for (arena_t *arena = heap->arenas.next; arena != end; arena = arena->next) {
        if ((flags & CMPCTMALLOC_ITERATE_UNUSED) != 0) {
            // The page starts with one arena and one sentinel header.
            uintptr_t first_possible_allocation = (uintptr_t)(arena + 1) + sizeof(header_t);
            // If this is the arena right after the heap structure, then do the
            // callback for the overhead of the heap structure itself at this
            // moment that the callback sees it while expecting callbacks for
            // the correct page.
            void *start_of_overhead;
            if (arena == heap->end_of_heap_structure) {
                start_of_overhead = heap;
                iterate_heap_structure = false;
            } else {
                start_of_overhead = arena;
            }
            callback(user_data, (void *)CMPCTMALLOC_ITERATE_TAG_HEAP_OVERHEAD, start_of_overhead, first_possible_allocation - (uintptr_t)start_of_overhead);
        }
        header_t *previous = (header_t *)(arena + 1);
        for (header_t *header = previous + 1; true; header = right_header(header)) {
            ASSERT(left_header(header) == previous);
            previous = header;
            if ((flags & CMPCTMALLOC_ITERATE_UNUSED) != 0) {
                callback(user_data, (void *)CMPCTMALLOC_ITERATE_TAG_HEAP_OVERHEAD, header, sizeof(header_t));
            }
            if (is_end_of_page_allocation(header)) break;
            if (is_tagged_as_free(header)) {
                if ((flags & CMPCTMALLOC_ITERATE_UNUSED) != 0) {
                    callback(user_data, (void *)CMPCTMALLOC_ITERATE_TAG_FREE, header + 1, get_size(header) - sizeof(header_t));
                }
            } else {
                if ((flags & CMPCTMALLOC_ITERATE_ALL_ALLOCATIONS) != 0 || header->tag == tag) {
                    if (callback(user_data, header->tag, header + 1, get_size(header) - sizeof(header_t))) {
                        // Callback returned true, so the allocation should be freed.
                        // We free with a delay so that it does not disturb the iteration.
                        if (to_free) {
                            cmpct_free_already_locked(heap, to_free + 1);
                        }
                        to_free = header;
                    }
                }
            }
        }
    }
    if (iterate_heap_structure) {
        callback(user_data, (void *)CMPCTMALLOC_ITERATE_TAG_HEAP_OVERHEAD, heap, (uintptr_t)heap->end_of_heap_structure - (uintptr_t)heap);
    }
    if (to_free) {
        cmpct_free_already_locked(heap, to_free + 1);
    }
    if ((flags & CMPCTMALLOC_ITERATE_UNLOCKED) == 0) {
        unlock(heap);
    }
}

#ifdef TEST_CMPCTMALLOC
/* Run the tests with:
gcc -m32 -ffreestanding -fsanitize=address -DTEST_CMPCTMALLOC -DDEBUG=1 -g -o test third_party/esp-idf/components/heap/third_party/dartino/cmpctmalloc.c -pthread && ./test
gcc      -ffreestanding -fsanitize=address -DTEST_CMPCTMALLOC -DDEBUG=1 -g -o test third_party/esp-idf/components/heap/third_party/dartino/cmpctmalloc.c -pthread && ./test
 */

int main(int argc, char *argv[])
{
    int TEST_HEAP_SIZE = 900000;
    void *arena = malloc(TEST_HEAP_SIZE);
    cmpct_heap_t *heap = cmpct_register_impl(arena, TEST_HEAP_SIZE);
    cmpct_test_get_back_newly_freed(heap);
    pthread_key_create(&tls_key, NULL);
    cmpct_test_tagged_allocations(heap);
    for (int i = 0; i < 1000; i++) {
        cmpct_test_churn(heap);
    }

    for (int size = 0; size < 12000; size++) {
        int first_success = -1;
        arena = malloc(size);
        heap = cmpct_register_impl(arena, size);
        if (heap == NULL) {
            ASSERT(size < sizeof(cmpct_heap_t));
            free(arena);
            continue;
        }
        for (int allocation = SMALL_ALLOCATION_LIMIT; allocation >= 1; allocation--) {
            void *p = cmpct_alloc(heap, allocation);
            if (p) {
                if (first_success == -1) {
                    first_success = allocation;
                }
                memset(p, 0x77, allocation);
            } else {
                if (first_success != -1) {
                    printf("At size %d, allocation failed at size %d\n", size, allocation);
                }
            }
            cmpct_free(heap, p);
        }
        // If the area we gave to the allocator was big enough then we would
        // expect an allocation to succeed, but the memory area can be split
        // over two pages, and so we need twice the minimum before we can be
        // sure of any allocations.
        uintptr_t minimum_arena = arena_overhead + 4 * sizeof(header_t);
        ASSERT(ROUND_DOWN(size, (int)sizeof(header_t)) <= sizeof(cmpct_heap_t) + minimum_arena * 2 || first_success != -1);
        free(arena);
    }
}

#endif  // TEST_CMPCTMALLOC

#ifdef CMPCTMALLOC_ON_LINUX
/*
# Create dynamic malloc library with:
FLAGS="-DCMPCTMALLOC_ON_LINUX -fPIC -ffreestanding -shared"
CFILE=third_party/esp-idf/components/heap/third_party/dartino/cmpctmalloc.c
gcc -m32 $FLAGS -g -o build/debug/bin/cmpctmalloc_32.so $CFILE
gcc      $FLAGS -g -o build/debug64/bin/cmpctmalloc_64.so $CFILE
gcc -m32 $FLAGS -O3 -o build/release/bin/cmpctmalloc_32.so $CFILE
gcc      $FLAGS -O3 -o build/release64/bin/cmpctmalloc_64.so $CFILE
# Then use it with:
LD_PRELOAD=`pwd`/build/debug64/bin/cmpctmalloc_64.so ./build/debug64/bin/toitc hw.toit
# or:
CMPCTMALLOC_DUMP_ON_EXIT=1 LD_PRELOAD=`pwd`/build/debug64/bin/cmpctmalloc_64.so ./build/debug64/bin/toitc hw.toit
 */

static cmpct_heap_t *heap = NULL;

void dump_on_exit()
{
    if (heap != NULL) cmpct_dump(heap);
}

static cmpct_heap_t *initial_heap()
{
    uintptr_t size = 1 << 30;  // 1 Gigabyte.
    void *pages = NULL;
    while (true) {
        pages = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, 0, 0);
        if (pages == NULL) {
            size >>= 1;
        } else {
            break;
        }
    }
    cmpct_heap_t *heap = cmpct_register_impl(pages, size);
    if (getenv("CMPCTMALLOC_DUMP_ON_EXIT") != NULL) atexit(dump_on_exit);
    pthread_key_create(&tls_key, NULL);
    return heap;
}

void *malloc(size_t size)
{
    if (heap == NULL) heap = initial_heap();
    return cmpct_malloc_impl(heap, size);
}

// Count leading zeros.
static int clzs(size_t size)
{
    ASSERT(size != 0);  // __builtin_clz is undefined for zero input.
    if (sizeof(size_t) == sizeof(int)) {
        return __builtin_clz(size);
    } else if (sizeof(size_t) == sizeof(long)) {
        return __builtin_clzl(size);
    } else if (sizeof(size_t) == sizeof(long long)) {
        return __builtin_clzll(size);
    } else {
        FATAL("Strange C compiler");
    }
}

void *calloc(size_t nelem, size_t elsize)
{
    if (heap == NULL) heap = initial_heap();
    size_t size;
    if (__builtin_mul_overflow (elsize, nelem, &size)) return NULL;
    void *ptr = malloc(size);
    if (ptr != NULL) memset(ptr, 0, size);
    return ptr;
}

void free(void *ptr)
{
    ASSERT(heap != NULL);
    return cmpct_free_impl(heap, ptr);
}

void *realloc(void *old, size_t size)
{
    if (heap == NULL) heap = initial_heap();
    return cmpct_realloc_impl(heap, old, size);
}

typedef bool heap_caps_iterate_callback(void *, void *, void *, size_t);
void heap_caps_iterate_tagged_memory_areas(void *user_data, void *tag, heap_caps_iterate_callback callback, int flags)
{
    cmpct_iterate_tagged_memory_areas(heap, user_data, tag, callback, flags);
}

#endif
