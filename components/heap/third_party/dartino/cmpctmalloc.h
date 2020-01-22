// Copyright (C) 2019 Toitware ApS. All rights reserved.

#pragma once

#include "../../include/multi_heap.h"

multi_heap_handle_t cmpct_register_impl(void *start, size_t size);
size_t cmpct_free_size_impl(multi_heap_handle_t heap);
size_t cmpct_get_allocated_size_impl(multi_heap_handle_t heap, void *p);
size_t cmpct_minimum_free_size_impl(multi_heap_handle_t heap);
void cmpct_free_impl(multi_heap_handle_t heap, void *p);
void cmpct_get_info_impl(multi_heap_handle_t heap, multi_heap_info_t *info);
void *cmpct_malloc_impl(multi_heap_handle_t heap, size_t size);
void *cmpct_realloc_impl(multi_heap_handle_t heap, void *p, size_t size);
void cmpct_iterate_tagged_memory_areas(multi_heap_handle_t heap, void *user_data, void *tag, tagged_memory_callback_t callback, int flags);

// Flags for heap iteration.  Coordinate with constants in esp_heap_caps.h
#define CMPCTMALLOC_ITERATE_UNLOCKED 1
#define CMPCTMALLOC_ITERATE_ALL_ALLOCATIONS 2
#define CMPCTMALLOC_ITERATE_UNUSED   4

#define CMPCTMALLOC_ITERATE_TAG_FREE          (-1)  /// Memory is free and could be allocated.
#define CMPCTMALLOC_ITERATE_TAG_HEAP_OVERHEAD (-2)  /// Memory is used by malloc for internal accounting etc.
