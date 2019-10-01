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

#ifdef TEST_CMPCTMALLOC

#define LTRACEF(...)
#define LTRACE_ENTRY
#define DEBUG_ASSERT assert
#define ASSERT(x) if (!(x)) abort()
#define USE(x) ((void)(x))
#define STATIC_ASSERT(condition)
#define dprintf(...) fprintf(__VA_ARGS__)
#define INFO stderr
#define IRAM_ATTR
#define MULTI_HEAP_LOCK(x)
#define MULTI_HEAP_UNLOCK(x)
#define GET_THREAD_LOCAL_TAG current_test_tag

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

void multi_heap_iterate_tagged_memory_areas(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback)
    __attribute__((alias("cmpct_iterate_tagged_memory_areas")));

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
#define GET_THREAD_LOCAL_TAG pvTaskGetThreadLocalStoragePointer(NULL, MULTI_HEAP_THREAD_TAG_INDEX)

#endif  // TEST_CMPCTMALLOC

#define ROUNDUP(x, alignment) (((x) + (alignment) - 1) & ~((alignment) - 1))
#define IS_ALIGNED(x, alignment) (((x) & ((alignment) - 1)) == 0)
#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))
// Provoke crash.
#define FATAL(reason) abort()
#define INLINE __attribute__((always_inline)) inline

// This is a two layer allocator.  Allocations > 4048 bytes are allocated from
// a block allocator that gives out 4k aligned blocks.  Those less than 4048
// bytes are allocated from cmpctmalloc.  This implementation of cmpctmalloc
// only requests blocks from the block allocator that are 4k large, thus no
// pointer returned from cmpctmalloc can be 4k aligned, but all allocations
// of 4k or more are 4k aligned.

// Malloc implementation tuned for space.
//
// Allocation strategy takes place with a global mutex.  Freelist entries are
// kept in linked lists with 8 different sizes per binary order of magnitude
// and the header size is two words with eager coalescing on free.

void *cmpct_alloc(cmpct_heap_t *heap, size_t size);
void cmpct_free(cmpct_heap_t *heap, void *payload);
size_t cmpct_free_size_impl(cmpct_heap_t *heap);
size_t cmpct_get_allocated_size_impl(cmpct_heap_t *heap, void *p);
static void *page_alloc(cmpct_heap_t *heap, intptr_t pages, void *tag);
static void page_free(cmpct_heap_t *heap, void *address, int pages_dummy);
struct header_struct;
static inline struct header_struct *right_header(struct header_struct *header);
static inline struct header_struct *left_header(struct header_struct *header);

#ifdef DEBUG
#define CMPCT_DEBUG
#endif

#define ALLOC_FILL 0x99
#define FREE_FILL 0x77
#define PADDING_FILL 0x55

#define HEAP_GROW_SIZE (4 * 1024) /* Grow only one page at a time. */

#define PAGE_SIZE_SHIFT 12
#define PAGE_SIZE (1 << PAGE_SIZE_SHIFT)
#define SMALL_ALLOCATION_LIMIT 4032
#define IS_PAGE_ALIGNED(x) (((uintptr_t)(x) & (PAGE_SIZE - 1)) == 0)
#define PAGES_FOR_BYTES(x) (((x) + PAGE_SIZE - 1) >> PAGE_SIZE_SHIFT)

STATIC_ASSERT(IS_PAGE_ALIGNED(HEAP_GROW_SIZE))

// Individual allocations above 4kbytes are just fetched directly from the
// block allocator.
#define HEAP_ALLOC_VIRTUAL_BITS 12

// When we grow the heap we have to have somewhere in the freelist to put the
// resulting freelist entry, so the freelist has to have a certain number of
// buckets.
STATIC_ASSERT(HEAP_GROW_SIZE <= (1u << HEAP_ALLOC_VIRTUAL_BITS))

// Buckets for allocations.  The smallest 15 buckets are 8, 16, 24, etc. up to
// 120 bytes.  After that we round up to the nearest size that can be written
// /^0*1...0*$/, giving 8 buckets per order of binary magnitude.  The freelist
// entries in a given bucket have at least the given size, plus the header
// size.  On 64 bit, the 8 byte bucket is useless, since the freelist header
// is 16 bytes larger than the header, but we have it for simplicity.
#define NUMBER_OF_BUCKETS (1 + 15 + (HEAP_ALLOC_VIRTUAL_BITS - 7) * 8)

// All individual memory areas on the heap start with this.
typedef struct header_struct {
    // This is divided up into two 16 bit fields, size and left_size.  We don't use actual
    // 16 bit fields because they don't work in IRAM, which is 32 bit only.
    // left_size: Used to find the previous memory area in address order.
    // size: For the next memory area.  Both size fields include the header.
    size_t size_;
    void *tag;  // Used for the process pointer.
} header_t;

static INLINE size_t get_left_size(header_t *header) {
    // The mask here should be 0xffff, but that would cause gcc to emit a 16 bit
    // load. We know the lowest bit of the size (top 16 bits) is always 0 so we
    // just include it here.
    return header->size_ & 0x1ffff;
}

static INLINE void set_left_size(header_t *header, size_t size) {
    ASSERT(size <= 0xffff);
    header->size_ = (header->size_ & ~0xffff) | size;
}

static INLINE size_t get_size(header_t *header) {
    size_t field = header->size_;
    // We should just shift down by 16, but that would cause gcc to emit a 16 bit
    // load. We know the highest bit of the left_size (bottom 16 bits) is always 0
    // so we extract that and add it in for no effect.  The expression (field >>
    // 15) & 1 can be obtained with a single extui instruction.  The more obvious
    // (field & 0x8000) would require a load of the 0x8000 constant from the
    // constant pool.
    return (field >> 16) + ((field >> 15) & 1);
}

static INLINE void set_size(header_t *header, size_t size) {
    ASSERT(size <= 0xffff);
    ASSERT((size & 1) == 0);
    header->size_ = (header->size_ & 0xffff) | (size << 16);
}

typedef struct free_struct {
    header_t header;
    struct free_struct *next;  // Double linked list of free areas in the same bucket.
    struct free_struct *prev;
} free_t;

// For page allocator, not originally part of cmpctmalloc.  These fields are 32 bit
// so that they work in IRAM on ESP32.
typedef struct Page {
    uint32_t continued;  // Boolean - this is part 2 or more of a series of pages.
    uint32_t in_use;     // Boolean.
    void *tag;
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
    void *lock;
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

static ssize_t heap_grow(cmpct_heap_t *heap, free_t **bucket);

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
    unsigned row = sizeof(size_t) * 8 - 4 - __builtin_clzl(size);
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
    size_t rounded = ROUNDUP(size, 8);
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
    for (index = ROUNDUP(index + 1, 32); index <= NUMBER_OF_BUCKETS; index += 32) {
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
    set_left_size(right, (char*)right - (char*)new_left + tag);
}

IRAM_ATTR static void unlink_free(cmpct_heap_t *heap, free_t *free_area, int bucket)
{
    if (get_size(&free_area->header) >= (1 << HEAP_ALLOC_VIRTUAL_BITS)) FATAL("Invalid free");
    heap->remaining -= get_size(&free_area->header);
    heap->free_blocks--;
    ASSERT(heap->remaining < 4000000000u);
    ASSERT(heap->free_blocks < 4000000000u);
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
            is_start_of_page_allocation(left) &&
            is_end_of_page_allocation(right)) {
        // The entire page was free and can be returned to the page allocator.
        unlink_free_unknown_bucket(heap, (free_t*)header);
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
    unsigned bucket;
    // Check for the 8-spaced buckets up to 128.
    for (unsigned i = 1; i <= 128; i++) {
        // Round up when allocating.
        bucket = size_to_index_allocating(i, &rounded);
        unsigned expected = (ROUNDUP(i, 8) >> 3) - 1;
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
            unsigned expected = bucket_base + ROUNDUP(i, j) / j;
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
    for (size_t i = 128; i <= SMALL_ALLOCATION_LIMIT; i *= 2, increment *= 2) {
        for (size_t j = i; j < i * 2; j += increment) {
            cmpct_test_get_back_newly_freed_helper(heap, i - 8);
            cmpct_test_get_back_newly_freed_helper(heap, i);
            cmpct_test_get_back_newly_freed_helper(heap, i + 1);
        }
    }
    for (size_t i = 1024; i <= 2048; i++) {
        cmpct_test_get_back_newly_freed_helper(heap, i);
    }
}

typedef struct cmpct_test_visit_record_struct {
    bool visited;
    void *address;
    size_t size;
} cmpct_test_visit_record;

static bool cmpct_test_visitor_no_free(void *r, void *tag, void *address, size_t size)
{
    if (tag != NULL) {
        cmpct_test_visit_record *record = (cmpct_test_visit_record *)r;
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

static void *current_test_tag = 0;

// For testing - doesn't use thread local storage.
static void cmpct_test_set_thread_tag(void *tag)
{
    current_test_tag = tag;
}

static void cmpct_test_tagged_allocations(cmpct_heap_t *heap)
{
    cmpct_test_set_thread_tag(NULL);  // From here, allocations are not tagged.
    void *alloc_1 = cmpct_alloc(heap, 123);
    void *alloc_2 = cmpct_alloc(heap, 123);
    cmpct_test_visit_record record;
    record.visited = false;

    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_no_free);
    ASSERT(!record.visited);  // No allocations were tagged.

    cmpct_test_set_thread_tag(&record);  // From here, allocations are tagged.

    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_no_free);
    ASSERT(!record.visited);  // Still no tagged allocations.

    void *alloc_3 = cmpct_alloc(heap, 123);  // This allocation will be tagged.
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_no_free);
    ASSERT(record.visited);  // We found the tagged allocation.
    ASSERT(record.address == alloc_3);
    ASSERT(record.size >= 123);

    record.visited = false;
    record.address = NULL;
    record.size = 0;

    // Use null tag so all allocations are iterated over, but the callback ignores those
    // with a null tag.
    cmpct_iterate_tagged_memory_areas(heap, &record, NULL, cmpct_test_visitor_no_free);
    ASSERT(record.visited);  // We found the tagged allocation.
    ASSERT(record.address == alloc_3);
    ASSERT(record.size >= 123);

    record.visited = false;
    record.address = NULL;
    record.size = 0;

    cmpct_free(heap, alloc_3);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_no_free);
    ASSERT(!record.visited);  // The allocation was freed, so it is not found.

    cmpct_free(heap, alloc_1);
    cmpct_free(heap, alloc_2);

    alloc_1 = cmpct_alloc(heap, 123);  // Create new tagged allocation.

    record.visited = false;
    record.address = NULL;
    record.size = 0;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free);  // Use freeing callback.
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_1);
    ASSERT(record.size >= 123);

    record.visited = false;
    record.address = NULL;
    record.size = 0;
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free);
    ASSERT(!record.visited);  // It was freed last time so it's gone now.

    // Create big tagged allocations that are put on the page heap.
    alloc_1 = cmpct_malloc_impl(heap, PAGE_SIZE);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free);
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_1);
    ASSERT(record.size == PAGE_SIZE);

    alloc_2 = cmpct_malloc_impl(heap, PAGE_SIZE * 3 / 2);
    cmpct_iterate_tagged_memory_areas(heap, &record, &record, cmpct_test_visitor_free);
    ASSERT(record.visited);  // It was found.
    ASSERT(record.address == alloc_2);
    ASSERT(record.size == PAGE_SIZE * 2);
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
        if (sizeof(void*) == 4) {
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

IRAM_ATTR void *cmpct_alloc(cmpct_heap_t *heap, size_t size)
{
    if (size == 0u) return NULL;

    ASSERT(size <= SMALL_ALLOCATION_LIMIT);

    size_t rounded_up;
    int start_bucket = size_to_index_allocating(size, &rounded_up);

    rounded_up += sizeof(header_t);

    lock(heap);
    int bucket = find_nonempty_bucket(heap, start_bucket);
    if (bucket == -1) {
        // Grow heap by one page. If we can.
        if (heap_grow(heap, NULL) < 0) {
            unlock(heap);
            return NULL;
        }
        bucket = find_nonempty_bucket(heap, start_bucket);
        // Allocation is always less than one page so this must succeed.
        ASSERT(bucket != -1);
    }
    free_t *head = heap->free_lists[bucket];
    header_t *block = &head->header;
    size_t block_size = get_size(block);
    size_t rest = get_size(block) - rounded_up;
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
        header_t *free = (header_t*)((char *)head + rounded_up);
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
        ((int*)(result))[i >> 2] = 0;
    }
#ifdef CMPCT_DEBUG
    memset(result, ALLOC_FILL, size);
    memset(((char *)result) + size, PADDING_FILL, rounded_up - size - sizeof(header_t));
#endif
    unlock(heap);
    return result;
}

IRAM_ATTR void cmpct_free_optionally_locked(cmpct_heap_t *heap, void *payload, bool use_locking)
{
    if (payload == NULL) return;
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

IRAM_ATTR static void add_to_heap(cmpct_heap_t *heap, void *new_area, size_t size, free_t **bucket)
{
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
    size_t free_size = size - 2 * sizeof(header_t) - sizeof(arena_t);
    create_free_area(heap, new_header, sizeof(header_t), free_size, bucket);
    header_t *right_sentinel = (header_t *)(top - sizeof(header_t));
    // Not free, stops attempts to coalesce right.
    create_allocation_header(right_sentinel, 0, free_size, NULL);
}

// Grab a page of memory from the page allocator.
// Called with the lock, apart from during init.
IRAM_ATTR static ssize_t heap_grow(cmpct_heap_t *heap, free_t **bucket)
{
    ASSERT(HEAP_GROW_SIZE == PAGE_SIZE);
    // Allocate one page more.  The allocation tag is a pointer to the heap
    // itself so that it won't match any allocation tag used by the program.
    void *ptr = page_alloc(heap, 1, heap);
    if (ptr == NULL) return -1;
    heap->size += PAGE_SIZE;
    LTRACEF("growing heap by 0x%zx bytes, new ptr %p\n", size, ptr);
    add_to_heap(heap, ptr, PAGE_SIZE, bucket);
    return PAGE_SIZE;
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
    heap->remaining = 0;
    heap->free_blocks = 0;
    heap->allocated_blocks = 0;
    // Empty doubly linked list points to itself.
    heap->arenas.previous = &heap->arenas;
    heap->arenas.next = &heap->arenas;
}

// Takes a memory area for a heap.  The first part of the memory that was given
// is used for an instance of cmpct_heap_t with the bookkeeping information for
// the heap.  The whole pages are added to the page allocator for this heap.
// Any space left after the whole pages is wasted, but space between the
// cmpct_heap_t and the first page is put on the freelist.  It will never be
// returned to the freelist because only page aligned free entries trigger the
// code that detects wholly empty pages and returns them to the page allocator.
cmpct_heap_t *cmpct_register_impl(void *start, size_t size)
{
    ASSERT(size > sizeof(cmpct_heap_t));
    // We can't have more pages than the rounded down size.
    intptr_t pages = size >> PAGE_SIZE_SHIFT;
    ASSERT(pages > 0);
    ASSERT((sizeof(cmpct_heap_t) + pages * sizeof(Page)) < PAGE_SIZE);

    uintptr_t start_int = (uintptr_t)start;
    intptr_t header_waste = ROUNDUP(start_int, sizeof(header_t)) - start_int;
    start_int += header_waste;
    uintptr_t end_int = (uintptr_t)start + size - header_waste;
    uintptr_t end_of_struct = start_int + sizeof(cmpct_heap_t) + pages * sizeof(Page);
    ASSERT(end_of_struct < end_int);
    uintptr_t start_of_first_page = ROUNDUP(end_of_struct, PAGE_SIZE);
    ASSERT(start_of_first_page < end_int);

    // May be a little smaller than the old value of pages.
    ASSERT((end_int - start_of_first_page) >> PAGE_SIZE_SHIFT <= pages);
    pages = (end_int - start_of_first_page) >> PAGE_SIZE_SHIFT;

    cmpct_heap_t *page_heap = (cmpct_heap_t*)start_int;
    page_heap->number_of_pages = pages;
    page_heap->page_base = (char*)start_of_first_page;
    for (size_t i = 0; i < pages; i++) {
        page_heap->pages[i].continued = 0;
        page_heap->pages[i].in_use = 0;
        page_heap->pages[i].tag = NULL;
    }
    // A sentinel page is in_use, but not a continuation of any previous
    // allocation.  This is just in the index, we don't actually waste a page
    // on this.
    page_heap->pages[pages].continued = 0;
    page_heap->pages[pages].in_use = 1;
    page_heap->pages[pages].tag = NULL;

    cmpct_init(page_heap);

    uintptr_t rest_of_zeroth_page = ROUNDUP(end_of_struct, sizeof(header_t));
    intptr_t waste = start_of_first_page - rest_of_zeroth_page;
    if (waste > sizeof(arena_t) + sizeof(free_t) * 3) {
        add_to_heap(page_heap, (void*)rest_of_zeroth_page, waste, NULL);
    }

    return page_heap;
}

IRAM_ATTR void *cmpct_malloc_impl(cmpct_heap_t *heap, size_t size)
{
    if (size <= SMALL_ALLOCATION_LIMIT) {
        return cmpct_alloc(heap, size);
    }
    lock(heap);
    void *result = page_alloc(heap, PAGES_FOR_BYTES(size), GET_THREAD_LOCAL_TAG);
    unlock(heap);
    return result;
}

IRAM_ATTR static bool is_page_allocated(void *p)
{
    return p != NULL && ((size_t)p & (PAGE_SIZE - 1)) == 0;
}

IRAM_ATTR void cmpct_free_impl(cmpct_heap_t *heap, void *p)
{
    if (is_page_allocated(p)) {
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
    ASSERT(is_page_allocated(p));
    size_t page = offset >> PAGE_SIZE_SHIFT;
    ASSERT(heap->pages[page].in_use == 1);
    ASSERT(heap->pages[page].continued == 0);
    return page;
}

IRAM_ATTR size_t cmpct_get_allocated_size_impl(cmpct_heap_t *heap, void *p)
{
    if (p == NULL) return 0;
    if (!is_page_allocated(p)) {
        size_t size = allocation_size(p);
        return size;
    }
    size_t page = page_number(heap, p);
    for (size_t i = 1; true; i++) {
        if (!heap->pages[page + i].continued) return i << PAGE_SIZE_SHIFT;
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
    info->total_free_bytes = heap->remaining;
    info->total_allocated_bytes = heap->size - heap->remaining;
    info->largest_free_block = 0;
    // TODO: We don't currently keep track of the all-time low number of free
    // bytes.
    info->minimum_free_bytes = 0;
    info->allocated_blocks = heap->allocated_blocks;
    info->free_blocks = heap->free_blocks;
    info->total_blocks = heap->free_blocks + heap->allocated_blocks;
    size_t current_page_run = 0;
    // Include sentinel in iteration.
    for (size_t i = 0; i <= heap->number_of_pages; i++) {
        if (heap->pages[i].in_use) {
            if (current_page_run > info->largest_free_block) {
                info->largest_free_block = current_page_run;
            }
            current_page_run = 0;
        } else {
            current_page_run += PAGE_SIZE;
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
        if (!heap->pages[i].in_use) {
            bool big_enough = true;
            for (int j = 1; j < pages; j++) {
                if (heap->pages[i + j].in_use) {
                    big_enough = false;
                    i += j;
                    break;
                }
            }
            if (big_enough) {
                heap->pages[i].in_use = 1;
                heap->pages[i].tag = tag;
                ASSERT(heap->pages[i].continued == 0);
                for (int j = 1; j < pages; j++) {
                    heap->pages[i + j].in_use = 1;
                    heap->pages[i + j].continued = 1;
                }
                void *result = heap->page_base + i * PAGE_SIZE;
                for (int i = 0; i < pages << PAGE_SIZE_SHIFT; i += sizeof(int)) {
                    ((int*)(result))[i >> 2] = 0;
                }
                return heap->page_base + i * PAGE_SIZE;
            }
        }
    }
    return NULL;
}

IRAM_ATTR static void page_iterate(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback)
{
    for (int i = 0; i < heap->number_of_pages; i++) {
        if (heap->pages[i].in_use && !heap->pages[i].continued) {
            // A null tag indicates that we should iterate over all allocations, but we still
            // don't iterate over the page allocations that the sub-page allocator made.
            if (heap->pages[i].tag == tag || (tag == NULL && heap->pages[i].tag != heap)) {
                for (int j = 1; true; j++) {
                    ASSERT(i + j <= heap->number_of_pages);
                    if (!heap->pages[i + j].continued) {
                        void *allocation = heap->page_base + i * PAGE_SIZE;
                        if (callback(user_data, heap->pages[i].tag, allocation, j * PAGE_SIZE)) {
                            // Callback indicates we should free the memory.
                            page_free(heap, allocation, j);
                            i += j - 1;
                            break;
                        }
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
    if (page >= heap->number_of_pages || !heap->pages[page].in_use || heap->pages[page].continued) {
        FATAL("Invalid free");
    }
    for (intptr_t j = page + 1; heap->pages[j].continued; j++) {
        heap->pages[j].in_use = 0;
        heap->pages[j].continued = 0;
    }
    heap->pages[page].in_use = 0;
    heap->pages[page].continued = 0;
}

void multi_heap_set_lock(cmpct_heap_t *heap, void *lock)
{
    heap->lock = lock;
}

void cmpct_iterate_tagged_memory_areas(cmpct_heap_t *heap, void *user_data, void *tag, tagged_memory_callback_t callback)
{
    lock(heap);
    page_iterate(heap, user_data, tag, callback);
    arena_t *end = &heap->arenas;
    header_t *to_free = NULL;
    for (arena_t *arena = heap->arenas.next; arena != end; arena = arena->next) {
        for (header_t *header = (header_t *)(arena + 1) + 1;
             !is_end_of_page_allocation(header);
             header = right_header(header)) {
            if (!is_tagged_as_free(header) && (tag == NULL || header->tag == tag)) {
                if (callback(user_data, header->tag, header + 1, get_size(header))) {
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
    if (to_free) {
        cmpct_free_already_locked(heap, to_free + 1);
    }
    unlock(heap);
}

#ifdef TEST_CMPCTMALLOC

int main(int argc, char *argv[]) {
    int TEST_HEAP_SIZE = 900000;
    void *arena = malloc(TEST_HEAP_SIZE);
    cmpct_heap_t *heap = cmpct_register_impl(arena, TEST_HEAP_SIZE);
    cmpct_test_get_back_newly_freed(heap);
    cmpct_test_tagged_allocations(heap);
    for (int i = 0; i < 1000; i++) {
        cmpct_test_churn(heap);
    }
}

#endif
