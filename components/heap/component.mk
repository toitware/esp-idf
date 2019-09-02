#
# Component Makefile
#

COMPONENT_SRCDIRS := . third_party/dartino
COMPONENT_OBJS := heap_caps_init.o heap_caps.o heap_trace.o

ifdef CONFIG_CMPCT_MALLOC_HEAP
COMPONENT_OBJS += third_party/dartino/cmpctmalloc.o
endif

ifndef CONFIG_CMPCT_MALLOC_HEAP
COMPONENT_OBJS += multi_heap.o
endif

ifndef CONFIG_HEAP_POISONING_DISABLED
COMPONENT_OBJS += multi_heap_poisoning.o

ifdef CONFIG_HEAP_TASK_TRACKING
COMPONENT_OBJS += heap_task_info.o
endif
endif

ifdef CONFIG_HEAP_TRACING

WRAP_FUNCTIONS = calloc malloc free realloc heap_caps_malloc heap_caps_free heap_caps_realloc heap_caps_malloc_default heap_caps_realloc_default
WRAP_ARGUMENT := -Wl,--wrap=

COMPONENT_ADD_LDFLAGS = -l$(COMPONENT_NAME) $(addprefix $(WRAP_ARGUMENT),$(WRAP_FUNCTIONS))

endif
