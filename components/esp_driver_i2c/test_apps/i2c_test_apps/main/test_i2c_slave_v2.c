/*
 * SPDX-FileCopyrightText: 2024-2025 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Unlicense OR CC0-1.0
 */
#include <stdio.h>
#include <string.h>
#include "sdkconfig.h"
#include "unity.h"
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "esp_err.h"
#include "driver/i2c_master.h"
#include "driver/i2c_slave.h"
#include "esp_rom_gpio.h"
#include "esp_rom_sys.h"
#include "esp_log.h"
#include "test_utils.h"
#include "test_board.h"

#if CONFIG_IDF_TARGET_ESP32
#include "esp_intr_alloc.h"
#include "hal/i2c_ll.h"
#include "i2c_private.h"
#include "soc/i2c_struct.h"
#endif

#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE

TEST_CASE("I2C peripheral allocate slave all", "[i2c]")
{
    i2c_slave_dev_handle_t dev_handle[SOC_HP_I2C_NUM];
    for (int i = 0; i < SOC_HP_I2C_NUM; i++) {
        i2c_slave_config_t i2c_slv_config_1 = {
            .clk_source = I2C_CLK_SRC_DEFAULT,
            .i2c_port = -1,
            .scl_io_num = I2C_SLAVE_SCL_IO,
            .sda_io_num = I2C_SLAVE_SDA_IO,
            .slave_addr = ESP_SLAVE_ADDR,
            .send_buf_depth = DATA_LENGTH,
            .receive_buf_depth = DATA_LENGTH,
            .flags.enable_internal_pullup = true,
        };

        TEST_ESP_OK(i2c_new_slave_device(&i2c_slv_config_1, &dev_handle[i]));
    }
    i2c_slave_config_t i2c_slv_config_1 = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = -1,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DATA_LENGTH,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };
    i2c_slave_dev_handle_t dev_handle_2;

    TEST_ESP_ERR(ESP_ERR_NOT_FOUND, i2c_new_slave_device(&i2c_slv_config_1, &dev_handle_2));

    for (int i = 0; i < SOC_HP_I2C_NUM; i++) {
        TEST_ESP_OK(i2c_del_slave_device(dev_handle[i]));
    }

    // Get another one

    TEST_ESP_OK(i2c_new_slave_device(&i2c_slv_config_1, &dev_handle_2));
    TEST_ESP_OK(i2c_del_slave_device(dev_handle_2));
}

static QueueHandle_t event_queue;
static uint8_t *temp_data;
static size_t temp_len = 0;

typedef enum {
    I2C_SLAVE_EVT_RX,
    I2C_SLAVE_EVT_TX
} i2c_slave_event_t;

void disp_buf(uint8_t *buf, int len)
{
    int i;
    for (i = 0; i < len; i++) {
        printf("%02x ", buf[i]);
        if ((i + 1) % 16 == 0) {
            printf("\n");
        }
    }
    printf("\n");
}

static bool i2c_slave_request_cb(i2c_slave_dev_handle_t i2c_slave, const i2c_slave_request_event_data_t *evt_data, void *arg)
{
    BaseType_t xTaskWoken;
    i2c_slave_event_t evt = I2C_SLAVE_EVT_TX;
    xQueueSendFromISR(event_queue, &evt, &xTaskWoken);
    return xTaskWoken;
}

static bool i2c_slave_receive_cb(i2c_slave_dev_handle_t i2c_slave, const i2c_slave_rx_done_event_data_t *evt_data, void *arg)
{
    BaseType_t xTaskWoken;
    i2c_slave_event_t evt = I2C_SLAVE_EVT_RX;
    memcpy(temp_data, evt_data->buffer, evt_data->length);
    temp_len = evt_data->length;
    xQueueSendFromISR(event_queue, &evt, &xTaskWoken);
    return xTaskWoken;
}

static void i2c_slave_read_test_v2(void)
{
    i2c_slave_dev_handle_t handle;
    event_queue = xQueueCreate(2, sizeof(i2c_slave_event_t));
    assert(event_queue);
    temp_data = malloc(DATA_LENGTH);
    assert(temp_data);

    i2c_slave_config_t i2c_slv_config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DATA_LENGTH,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };

    TEST_ESP_OK(i2c_new_slave_device(&i2c_slv_config, &handle));

    i2c_slave_event_callbacks_t cbs = {
        .on_receive = i2c_slave_receive_cb,
        .on_request = i2c_slave_request_cb,
    };

    TEST_ESP_OK(i2c_slave_register_event_callbacks(handle, &cbs, NULL));

    unity_send_signal("i2c slave init finish");

    unity_wait_for_signal("master write");

    i2c_slave_event_t evt;
    if (xQueueReceive(event_queue, &evt, portMAX_DELAY) == pdTRUE) {
        if (evt == I2C_SLAVE_EVT_RX) {
            disp_buf(temp_data, temp_len);
            printf("length is %x\n", temp_len);
            for (int i = 0; i < temp_len; i++) {
                TEST_ASSERT(temp_data[i] == i);
            }
        }
    }

    unity_send_signal("ready to delete");
    free(temp_data);
    vQueueDelete(event_queue);
    TEST_ESP_OK(i2c_del_slave_device(handle));
}

static void i2c_master_write_test_v2(void)
{
    uint8_t data_wr[DATA_LENGTH] = { 0 };
    int i;

    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus_handle;

    TEST_ESP_OK(i2c_new_master_bus(&i2c_mst_config, &bus_handle));

    i2c_device_config_t dev_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = ESP_SLAVE_ADDR,
        .scl_speed_hz = 100000,
    };

    i2c_master_dev_handle_t dev_handle;
    TEST_ESP_OK(i2c_master_bus_add_device(bus_handle, &dev_cfg, &dev_handle));

    unity_wait_for_signal("i2c slave init finish");

    unity_send_signal("master write");
    for (i = 0; i < DATA_LENGTH; i++) {
        data_wr[i] = i;
    }

    disp_buf(data_wr, i);
    TEST_ESP_OK(i2c_master_transmit(dev_handle, data_wr, DATA_LENGTH, -1));
    unity_wait_for_signal("ready to delete");
    TEST_ESP_OK(i2c_master_bus_rm_device(dev_handle));

    TEST_ESP_OK(i2c_del_master_bus(bus_handle));
}

TEST_CASE_MULTIPLE_DEVICES("I2C master write slave v2 test", "[i2c][test_env=generic_multi_device][timeout=150]", i2c_master_write_test_v2, i2c_slave_read_test_v2);

static void master_read_slave_test_v2(void)
{
    uint8_t data_rd[DATA_LENGTH] = {0};
    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus_handle;
    TEST_ESP_OK(i2c_new_master_bus(&i2c_mst_config, &bus_handle));

    i2c_device_config_t dev_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = ESP_SLAVE_ADDR,
        .scl_speed_hz = 100000,
        .scl_wait_us = 20000,
    };

    i2c_master_dev_handle_t dev_handle;
    TEST_ESP_OK(i2c_master_bus_add_device(bus_handle, &dev_cfg, &dev_handle));

    unity_wait_for_signal("i2c slave init finish");

    TEST_ESP_OK(i2c_master_receive(dev_handle, data_rd, DATA_LENGTH, -1));
    vTaskDelay(100 / portTICK_PERIOD_MS);
    for (int i = 0; i < DATA_LENGTH; i++) {
        printf("%x\n", data_rd[i]);
        TEST_ASSERT(data_rd[i] == i);
    }
    unity_send_signal("ready to delete master read test");

    TEST_ESP_OK(i2c_master_bus_rm_device(dev_handle));
    TEST_ESP_OK(i2c_del_master_bus(bus_handle));
}

static void slave_write_buffer_test_v2(void)
{
    i2c_slave_dev_handle_t handle;
    uint8_t data_wr[DATA_LENGTH];
    event_queue = xQueueCreate(2, sizeof(i2c_slave_event_t));
    assert(event_queue);

    i2c_slave_config_t i2c_slv_config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DATA_LENGTH,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };

    TEST_ESP_OK(i2c_new_slave_device(&i2c_slv_config, &handle));

    i2c_slave_event_callbacks_t cbs = {
        .on_receive = i2c_slave_receive_cb,
        .on_request = i2c_slave_request_cb,
    };

    TEST_ESP_OK(i2c_slave_register_event_callbacks(handle, &cbs, NULL));

    unity_send_signal("i2c slave init finish");

    for (int i = 0; i < DATA_LENGTH; i++) {
        data_wr[i] = i;
    }

    i2c_slave_event_t evt;
    uint32_t write_len;
    while (true) {
        if (xQueueReceive(event_queue, &evt, portMAX_DELAY) == pdTRUE) {
            if (evt == I2C_SLAVE_EVT_TX) {
                TEST_ESP_OK(i2c_slave_write(handle, data_wr, DATA_LENGTH, &write_len, 1000));
                break;
            }
        }
    }

    unity_wait_for_signal("ready to delete master read test");
    vQueueDelete(event_queue);
    TEST_ESP_OK(i2c_del_slave_device(handle));
}

TEST_CASE_MULTIPLE_DEVICES("I2C master read slave test", "[i2c][test_env=generic_multi_device][timeout=150]", master_read_slave_test_v2, slave_write_buffer_test_v2);

static void i2c_master_write_test_with_customize_api(void)
{
    uint8_t data_wr[DATA_LENGTH] = { 0 };
    int i;

    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus_handle;

    TEST_ESP_OK(i2c_new_master_bus(&i2c_mst_config, &bus_handle));

    i2c_device_config_t dev_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = I2C_DEVICE_ADDRESS_NOT_USED,
        .scl_speed_hz = 100000,
    };

    i2c_master_dev_handle_t dev_handle;
    TEST_ESP_OK(i2c_master_bus_add_device(bus_handle, &dev_cfg, &dev_handle));

    unity_wait_for_signal("i2c slave init finish");

    unity_send_signal("master write");
    for (i = 0; i < DATA_LENGTH; i++) {
        data_wr[i] = i;
    }

    disp_buf(data_wr, i);

    uint8_t address = (ESP_SLAVE_ADDR << 1 | 0);

    i2c_operation_job_t i2c_ops[] = {
        { .command = I2C_MASTER_CMD_START },
        { .command = I2C_MASTER_CMD_WRITE, .write = { .ack_check = true, .data = (uint8_t *) &address, .total_bytes = 1 } },
        { .command = I2C_MASTER_CMD_WRITE, .write = { .ack_check = true, .data = (uint8_t *) data_wr, .total_bytes = DATA_LENGTH } },
        { .command = I2C_MASTER_CMD_STOP },
    };

    TEST_ESP_OK(i2c_master_execute_defined_operations(dev_handle, i2c_ops, sizeof(i2c_ops) / sizeof(i2c_operation_job_t), -1));
    unity_wait_for_signal("ready to delete");
    TEST_ESP_OK(i2c_master_bus_rm_device(dev_handle));

    TEST_ESP_OK(i2c_del_master_bus(bus_handle));
}
TEST_CASE_MULTIPLE_DEVICES("I2C master write slave with customize api", "[i2c][test_env=generic_multi_device][timeout=150]", i2c_master_write_test_with_customize_api, i2c_slave_read_test_v2);

static void master_read_slave_test_v2_single_byte(void)
{
    uint8_t data_rd[DATA_LENGTH] = {0};
    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus_handle;
    TEST_ESP_OK(i2c_new_master_bus(&i2c_mst_config, &bus_handle));

    i2c_device_config_t dev_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = I2C_DEVICE_ADDRESS_NOT_USED,
        .scl_speed_hz = 100000,
        .scl_wait_us = 20000,
    };

    i2c_master_dev_handle_t dev_handle;
    TEST_ESP_OK(i2c_master_bus_add_device(bus_handle, &dev_cfg, &dev_handle));

    unity_wait_for_signal("i2c slave init finish");

    uint8_t read_address = (ESP_SLAVE_ADDR << 1 | 1);

    i2c_operation_job_t i2c_ops[] = {
        { .command = I2C_MASTER_CMD_START },
        { .command = I2C_MASTER_CMD_WRITE, .write = { .ack_check = true, .data = (uint8_t *) &read_address, .total_bytes = 1 } },
        { .command = I2C_MASTER_CMD_READ, .read = { .ack_value = I2C_NACK_VAL, .data = data_rd, .total_bytes = 1 }},
        { .command = I2C_MASTER_CMD_STOP },
    };

    i2c_master_execute_defined_operations(dev_handle, i2c_ops, sizeof(i2c_ops) / sizeof(i2c_operation_job_t), 1000);
    vTaskDelay(100 / portTICK_PERIOD_MS);
    TEST_ASSERT(data_rd[0] == 6);
    unity_send_signal("ready to delete master read test");

    TEST_ESP_OK(i2c_master_bus_rm_device(dev_handle));
    TEST_ESP_OK(i2c_del_master_bus(bus_handle));
}

static void slave_write_buffer_test_v2_single_byte(void)
{
    i2c_slave_dev_handle_t handle;
    uint8_t data_wr[DATA_LENGTH];
    event_queue = xQueueCreate(2, sizeof(i2c_slave_event_t));
    assert(event_queue);

    i2c_slave_config_t i2c_slv_config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DATA_LENGTH,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };

    TEST_ESP_OK(i2c_new_slave_device(&i2c_slv_config, &handle));

    i2c_slave_event_callbacks_t cbs = {
        .on_receive = i2c_slave_receive_cb,
        .on_request = i2c_slave_request_cb,
    };

    TEST_ESP_OK(i2c_slave_register_event_callbacks(handle, &cbs, NULL));

    unity_send_signal("i2c slave init finish");

    data_wr[0] = 6;

    i2c_slave_event_t evt;
    uint32_t write_len;
    while (true) {
        if (xQueueReceive(event_queue, &evt, portMAX_DELAY) == pdTRUE) {
            if (evt == I2C_SLAVE_EVT_TX) {
                TEST_ESP_OK(i2c_slave_write(handle, data_wr, 1, &write_len, 1000));
                break;
            }
        }
    }

    unity_wait_for_signal("ready to delete master read test");
    vQueueDelete(event_queue);
    TEST_ESP_OK(i2c_del_slave_device(handle));
}

TEST_CASE_MULTIPLE_DEVICES("I2C master read slave test single byte", "[i2c][test_env=generic_multi_device][timeout=150]", master_read_slave_test_v2_single_byte, slave_write_buffer_test_v2_single_byte);

#define DEFAULT_RESPONSE_TEST_ITERATIONS 64
#define DEFAULT_RESPONSE_TEST_LENGTH 64

static const uint8_t s_default_response[] = { 0xd0, 0xd1, 0xd2, 0xd3 };
static const uint8_t s_updated_default_response[] = { 0xe0, 0xe1, 0xe2 };

static void master_default_response_test(void)
{
    i2c_master_bus_config_t bus_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus;
    TEST_ESP_OK(i2c_new_master_bus(&bus_config, &bus));

    i2c_device_config_t device_config = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = ESP_SLAVE_ADDR,
        .scl_speed_hz = 400000,
        .scl_wait_us = 20000,
    };
    i2c_master_dev_handle_t device;
    TEST_ESP_OK(i2c_master_bus_add_device(bus, &device_config, &device));

    unity_wait_for_signal("default response target ready");
    uint8_t default_data[sizeof(s_default_response)] = {};
    TEST_ESP_OK(i2c_master_receive(device, default_data, sizeof(default_data), -1));
    TEST_ASSERT_EQUAL_HEX8_ARRAY(s_default_response, default_data, sizeof(default_data));
    unity_send_signal("initial default consumed");

    uint8_t explicit_data[DEFAULT_RESPONSE_TEST_LENGTH];
    for (int iteration = 0; iteration < DEFAULT_RESPONSE_TEST_ITERATIONS; iteration++) {
        unity_wait_for_signal("explicit response queued");
        unity_send_signal("explicit read starting");
        TEST_ESP_OK(i2c_master_receive(device, explicit_data, sizeof(explicit_data), -1));
        for (size_t i = 0; i < sizeof(explicit_data); i++) {
            TEST_ASSERT_EQUAL_HEX8((uint8_t)(iteration + i), explicit_data[i]);
        }

        memset(default_data, 0, sizeof(default_data));
        TEST_ESP_OK(i2c_master_receive(device, default_data, sizeof(default_data), -1));
        TEST_ASSERT_EQUAL_HEX8_ARRAY(s_default_response, default_data, sizeof(default_data));
        unity_send_signal("explicit and fallback consumed");
    }

    unity_wait_for_signal("updated default ready");
    uint8_t updated_default[sizeof(s_updated_default_response)] = {};
    TEST_ESP_OK(i2c_master_receive(device, updated_default, sizeof(updated_default), -1));
    TEST_ASSERT_EQUAL_HEX8_ARRAY(s_updated_default_response, updated_default, sizeof(updated_default));
    unity_send_signal("default response test complete");

    TEST_ESP_OK(i2c_master_bus_rm_device(device));
    TEST_ESP_OK(i2c_del_master_bus(bus));
}

static void slave_default_response_test(void)
{
    i2c_slave_config_t config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DEFAULT_RESPONSE_TEST_LENGTH * 2,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };
    i2c_slave_dev_handle_t target;
    TEST_ESP_OK(i2c_new_slave_device(&config, &target));
    TEST_ESP_OK(i2c_slave_set_default_response(target, s_default_response, sizeof(s_default_response)));

    unity_send_signal("default response target ready");
    unity_wait_for_signal("initial default consumed");

    uint8_t explicit_data[DEFAULT_RESPONSE_TEST_LENGTH];
    for (int iteration = 0; iteration < DEFAULT_RESPONSE_TEST_ITERATIONS; iteration++) {
        for (size_t i = 0; i < sizeof(explicit_data); i++) {
            explicit_data[i] = (uint8_t)(iteration + i);
        }

        TEST_ESP_OK(i2c_slave_set_buffered_write_pending(target, true));
        uint32_t written = 0;
        TEST_ESP_OK(i2c_slave_write(target, explicit_data, sizeof(explicit_data), &written, -1));
        TEST_ASSERT_EQUAL(sizeof(explicit_data), written);
        unity_send_signal("explicit response queued");
        unity_wait_for_signal("explicit read starting");

        // Sweep across the first FIFO-drain boundary. This stresses arbitration
        // between TX_EMPTY on one core and clearing the producer state here.
        esp_rom_delay_us(500 + (iteration % 16) * 40);
        TEST_ESP_OK(i2c_slave_set_buffered_write_pending(target, false));
        unity_wait_for_signal("explicit and fallback consumed");
    }

    TEST_ESP_OK(i2c_slave_set_default_response(target, s_updated_default_response, sizeof(s_updated_default_response)));
    unity_send_signal("updated default ready");
    unity_wait_for_signal("default response test complete");
    TEST_ESP_OK(i2c_del_slave_device(target));
}

TEST_CASE_MULTIPLE_DEVICES("I2C slave default response arbitration", "[i2c][test_env=generic_multi_device][timeout=300]", master_default_response_test, slave_default_response_test);

static DRAM_ATTR uint8_t s_callback_response[64];
static volatile size_t s_callback_response_offset;
static volatile size_t s_callback_transmitted;
static SemaphoreHandle_t s_callback_done;

static IRAM_ATTR bool slave_transmit_callback(i2c_slave_dev_handle_t target,
                                              i2c_slave_transmit_event_data_t *event,
                                              void *context)
{
    size_t remaining = sizeof(s_callback_response) - s_callback_response_offset;
    size_t length = event->buffer_size < remaining ? event->buffer_size : remaining;
    event->buffer = &s_callback_response[s_callback_response_offset];
    event->length = length;
    s_callback_response_offset += length;
    return false;
}

static IRAM_ATTR bool slave_transmit_done_callback(i2c_slave_dev_handle_t target,
                                                   const i2c_slave_transmit_done_event_data_t *event,
                                                   void *context)
{
    BaseType_t task_woken = pdFALSE;
    s_callback_transmitted = event->length;
    s_callback_response_offset = 0;
    xSemaphoreGiveFromISR(s_callback_done, &task_woken);
    return task_woken == pdTRUE;
}

static const size_t s_callback_read_lengths[] = { 1, 7, 31, 32, 47 };

static void master_synchronous_callback_test(void)
{
    i2c_master_bus_config_t bus_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus;
    TEST_ESP_OK(i2c_new_master_bus(&bus_config, &bus));

    i2c_device_config_t device_config = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = ESP_SLAVE_ADDR,
        .scl_speed_hz = 400000,
        .scl_wait_us = 20000,
    };
    i2c_master_dev_handle_t device;
    TEST_ESP_OK(i2c_master_bus_add_device(bus, &device_config, &device));

    unity_wait_for_signal("synchronous callback target ready");
    uint8_t received[sizeof(s_callback_response)];
    for (size_t iteration = 0; iteration < sizeof(s_callback_read_lengths) / sizeof(s_callback_read_lengths[0]); iteration++) {
        size_t length = s_callback_read_lengths[iteration];
        memset(received, 0, sizeof(received));
        TEST_ESP_OK(i2c_master_receive(device, received, length, -1));
        for (size_t i = 0; i < length; i++) {
            TEST_ASSERT_EQUAL_HEX8((uint8_t)(0x40 + i), received[i]);
        }
        unity_send_signal("synchronous callback read complete");
        unity_wait_for_signal("synchronous callback checked");
    }

    TEST_ESP_OK(i2c_master_bus_rm_device(device));
    TEST_ESP_OK(i2c_del_master_bus(bus));
}

static void slave_synchronous_callback_test(void)
{
    i2c_slave_config_t config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = sizeof(s_callback_response),
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };
    i2c_slave_dev_handle_t target;
    TEST_ESP_OK(i2c_new_slave_device(&config, &target));

    for (size_t i = 0; i < sizeof(s_callback_response); i++) {
        s_callback_response[i] = (uint8_t)(0x40 + i);
    }
    s_callback_response_offset = 0;
    s_callback_transmitted = 0;
    s_callback_done = xSemaphoreCreateBinary();
    TEST_ASSERT_NOT_NULL(s_callback_done);

    i2c_slave_event_callbacks_t callbacks = {
        .on_transmit = slave_transmit_callback,
        .on_transmit_done = slave_transmit_done_callback,
    };
    TEST_ESP_OK(i2c_slave_register_event_callbacks(target, &callbacks, NULL));
    unity_send_signal("synchronous callback target ready");

    for (size_t iteration = 0; iteration < sizeof(s_callback_read_lengths) / sizeof(s_callback_read_lengths[0]); iteration++) {
        TEST_ASSERT_EQUAL(pdTRUE, xSemaphoreTake(s_callback_done, pdMS_TO_TICKS(1000)));
        TEST_ASSERT_EQUAL(s_callback_read_lengths[iteration], s_callback_transmitted);
        unity_wait_for_signal("synchronous callback read complete");
        unity_send_signal("synchronous callback checked");
    }

    TEST_ESP_OK(i2c_slave_register_event_callbacks(target, &(i2c_slave_event_callbacks_t) {}, NULL));
    vSemaphoreDelete(s_callback_done);
    s_callback_done = NULL;
    TEST_ESP_OK(i2c_del_slave_device(target));
}

TEST_CASE_MULTIPLE_DEVICES("I2C slave synchronous transmit callbacks", "[i2c][test_env=generic_multi_device][timeout=150]", master_synchronous_callback_test, slave_synchronous_callback_test);

#endif // SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE

#if CONFIG_IDF_TARGET_ESP32

#define STALE_COUNT_TEST_LENGTH 20
#define STALE_COUNT_REPETITIONS 3

typedef struct {
    size_t length;
    bool overflow;
    uint8_t data[STALE_COUNT_TEST_LENGTH * 2];
} stale_count_receive_event_t;

typedef struct {
    uint32_t raw_interrupts;
    uint32_t fifo_count;
    esp_err_t disable_result;
    esp_err_t enable_result;
    bool callback_received;
    stale_count_receive_event_t receive;
} stale_count_result_t;

static QueueHandle_t stale_count_queue;

static uint8_t stale_count_pattern(int repetition, int index)
{
    return (uint8_t)(repetition * 37 + index * 31 + 23);
}

static bool stale_count_receive_cb(i2c_slave_dev_handle_t handle,
                                   const i2c_slave_rx_done_event_data_t *event_data,
                                   void *user_data)
{
    (void)handle;
    (void)user_data;
    stale_count_receive_event_t event = {
        .length = event_data->length,
        .overflow = event_data->overflow,
    };
    size_t copy_length = event_data->length < sizeof(event.data) ? event_data->length : sizeof(event.data);
    memcpy(event.data, event_data->buffer, copy_length);
    BaseType_t task_woken = pdFALSE;
    xQueueSendFromISR(stale_count_queue, &event, &task_woken);
    return task_woken == pdTRUE;
}

static void i2c_slave_refresh_rx_count_test(void)
{
    stale_count_result_t results[STALE_COUNT_REPETITIONS] = {0};
    stale_count_queue = xQueueCreate(1, sizeof(stale_count_receive_event_t));
    TEST_ASSERT_NOT_NULL(stale_count_queue);

    i2c_slave_config_t slave_config = {
        .i2c_port = TEST_I2C_PORT,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .scl_io_num = I2C_SLAVE_SCL_IO,
        .sda_io_num = I2C_SLAVE_SDA_IO,
        .slave_addr = ESP_SLAVE_ADDR,
        .send_buf_depth = DATA_LENGTH,
        .receive_buf_depth = DATA_LENGTH,
        .flags.enable_internal_pullup = true,
    };
    i2c_slave_dev_handle_t slave = NULL;
    TEST_ESP_OK(i2c_new_slave_device(&slave_config, &slave));
    i2c_slave_event_callbacks_t callbacks = {
        .on_receive = stale_count_receive_cb,
    };
    TEST_ESP_OK(i2c_slave_register_event_callbacks(slave, &callbacks, NULL));

    for (int repetition = 0; repetition < STALE_COUNT_REPETITIONS; repetition++) {
        // Classic ESP32 has a 32-byte RX FIFO and a 16-byte watermark. Holding
        // only this target's I2C interrupt through STOP leaves 20 bytes in the
        // FIFO with watermark and completion co-latched, without overflowing.
        I2C0.int_clr.rx_fifo_ovf = 1;
        results[repetition].disable_result = esp_intr_disable(slave->base->intr_handle);
        unity_send_signal("target interrupt disabled");
        unity_wait_for_signal("master write complete");

        results[repetition].raw_interrupts = I2C0.int_raw.val;
        i2c_ll_get_rxfifo_cnt(slave->base->hal.dev, &results[repetition].fifo_count);
        results[repetition].enable_result = esp_intr_enable(slave->base->intr_handle);
        results[repetition].callback_received = xQueueReceive(
            stale_count_queue, &results[repetition].receive, pdMS_TO_TICKS(2000)) == pdTRUE;
        unity_send_signal("target capture complete");
    }

    TEST_ESP_OK(i2c_del_slave_device(slave));
    vQueueDelete(stale_count_queue);
    stale_count_queue = NULL;

    const uint32_t expected_interrupts = I2C_INTR_SLV_RXFIFO_WM | I2C_INTR_SLV_COMPLETE;
    for (int repetition = 0; repetition < STALE_COUNT_REPETITIONS; repetition++) {
        TEST_ESP_OK(results[repetition].disable_result);
        TEST_ESP_OK(results[repetition].enable_result);
        TEST_ASSERT_EQUAL_HEX32(expected_interrupts,
                                results[repetition].raw_interrupts & expected_interrupts);
        TEST_ASSERT_EQUAL_UINT32(STALE_COUNT_TEST_LENGTH, results[repetition].fifo_count);
        TEST_ASSERT_FALSE(results[repetition].raw_interrupts & I2C_RXFIFO_OVF_INT_RAW_M);
        TEST_ASSERT_TRUE(results[repetition].callback_received);
        TEST_ASSERT_FALSE(results[repetition].receive.overflow);
        TEST_ASSERT_EQUAL_UINT32(STALE_COUNT_TEST_LENGTH, results[repetition].receive.length);
        for (int index = 0; index < STALE_COUNT_TEST_LENGTH; index++) {
            TEST_ASSERT_EQUAL_HEX8(stale_count_pattern(repetition, index),
                                   results[repetition].receive.data[index]);
        }
    }
}

static void i2c_master_refresh_rx_count_test(void)
{
    i2c_master_bus_config_t bus_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = TEST_I2C_PORT,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus = NULL;
    TEST_ESP_OK(i2c_new_master_bus(&bus_config, &bus));

    i2c_device_config_t device_config = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = ESP_SLAVE_ADDR,
        .scl_speed_hz = 100000,
    };
    i2c_master_dev_handle_t device = NULL;
    TEST_ESP_OK(i2c_master_bus_add_device(bus, &device_config, &device));

    esp_err_t transmit_results[STALE_COUNT_REPETITIONS];
    for (int repetition = 0; repetition < STALE_COUNT_REPETITIONS; repetition++) {
        uint8_t data[STALE_COUNT_TEST_LENGTH];
        for (int index = 0; index < STALE_COUNT_TEST_LENGTH; index++) {
            data[index] = stale_count_pattern(repetition, index);
        }
        unity_wait_for_signal("target interrupt disabled");
        transmit_results[repetition] = i2c_master_transmit(device, data, sizeof(data), 1000);
        unity_send_signal("master write complete");
        unity_wait_for_signal("target capture complete");
    }

    TEST_ESP_OK(i2c_master_bus_rm_device(device));
    TEST_ESP_OK(i2c_del_master_bus(bus));
    for (int repetition = 0; repetition < STALE_COUNT_REPETITIONS; repetition++) {
        TEST_ESP_OK(transmit_results[repetition]);
    }
}

TEST_CASE_MULTIPLE_DEVICES("I2C slave refreshes RX FIFO count between interrupt causes",
                           "[i2c][test_env=generic_multi_device][timeout=150]",
                           i2c_master_refresh_rx_count_test, i2c_slave_refresh_rx_count_test);

#endif // CONFIG_IDF_TARGET_ESP32
