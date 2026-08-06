/*
 * SPDX-FileCopyrightText: 2024-2025 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "sdkconfig.h"
#include "soc/soc_caps.h"
#include "esp_attr.h"
#include "esp_rom_gpio.h"
#include "driver/gpio.h"
#include "hal/gpio_ll.h"
#include "esp_err.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/ringbuf.h"
#include "esp_intr_alloc.h"
#include "hal/i2c_ll.h"
#include "i2c_private.h"
#include "driver/i2c_slave.h"
#include "esp_memory_utils.h"
#if CONFIG_I2C_ENABLE_DEBUG_LOG
// The local log level must be defined before including esp_log.h
// Set the maximum log level for this source file
#define LOG_LOCAL_LEVEL ESP_LOG_DEBUG
#endif
#include "esp_log.h"
#include "esp_check.h"

static const char *TAG = "i2c.slave";

IRAM_ATTR static bool i2c_slave_read_rx(i2c_slave_dev_t *i2c_slave, uint8_t *data, size_t len, size_t *read_len)
{
    BaseType_t xTaskWoken = pdFALSE;
    size_t read_size = len;
    size_t actual_size = 0;
    size_t available_size = 0;
    size_t get_size = 0;
    uint8_t *rx_data = NULL;

    vRingbufferGetInfo(i2c_slave->rx_ring_buf, NULL, NULL, NULL, NULL, &available_size);
    if (available_size < read_size) {
        read_size = available_size;
    }

    while (read_size) {
        actual_size = 0;
        rx_data = (uint8_t *)xRingbufferReceiveUpToFromISR(i2c_slave->rx_ring_buf, &actual_size, read_size);
        if (rx_data != NULL && actual_size != 0) {
            memcpy(data + get_size, rx_data, actual_size);
            vRingbufferReturnItemFromISR(i2c_slave->rx_ring_buf, rx_data, &xTaskWoken);
            get_size += actual_size;
            read_size -= actual_size;
        } else {
            break;
        }
    }
    *read_len = get_size;
    return xTaskWoken;
}

IRAM_ATTR static bool i2c_slave_handle_tx_fifo(i2c_slave_dev_t *i2c_slave, size_t *loaded)
{
    BaseType_t xTaskWoken = pdFALSE;
    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    uint8_t *data;
    uint32_t fifo_len = 0;
    portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
    i2c_ll_get_txfifo_len(hal->dev, &fifo_len);
    while (i2c_slave->transmit_callback && fifo_len > 0) {
        i2c_slave_transmit_event_data_t edata = {
            .buffer = NULL,
            .buffer_size = fifo_len,
            .length = 0,
        };
        xTaskWoken |= i2c_slave->transmit_callback(i2c_slave, &edata, i2c_slave->user_ctx);
        size_t callback_length = edata.length < fifo_len ? edata.length : fifo_len;
        if (edata.buffer == NULL || callback_length == 0) {
            break;
        }
        portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
        i2c_ll_write_txfifo(hal->dev, edata.buffer, callback_length);
        i2c_slave->tx_data_count += callback_length;
        fifo_len -= callback_length;
        if (loaded) {
            *loaded += callback_length;
        }
        portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);
    }
    size_t actual_get_len = 0;
    while (fifo_len > 0) {
        data = xRingbufferReceiveUpToFromISR(i2c_slave->tx_ring_buf, &actual_get_len, fifo_len);
        if (data) {
            i2c_ll_write_txfifo(hal->dev, data, actual_get_len);
            fifo_len -= actual_get_len;
            if (loaded) {
                *loaded += actual_get_len;
            }
            vRingbufferReturnItemFromISR(i2c_slave->tx_ring_buf, data, &xTaskWoken);
        } else {
            // No data in ringbuffer, so disable the tx interrupt.
            i2c_ll_slave_disable_tx_it(hal->dev);
            break;
        }
    }
    portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);
    return xTaskWoken;
}

#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
IRAM_ATTR static bool i2c_slave_handle_tx_done(i2c_slave_dev_t *i2c_slave)
{
    if (!i2c_slave->transmit_callback) {
        return false;
    }

    BaseType_t xTaskWoken = pdFALSE;
    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    uint32_t free_fifo_len = 0;
    portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
    i2c_ll_slave_disable_tx_it(hal->dev);
    i2c_ll_get_txfifo_len(hal->dev, &free_fifo_len);
    uint32_t remaining = SOC_I2C_FIFO_LEN - free_fifo_len;
    uint32_t removed = i2c_slave->tx_data_count > remaining
        ? i2c_slave->tx_data_count - remaining
        : 0;
    // The slave peripheral removes the next byte from the FIFO before it sees
    // the controller's terminal NACK. That prefetched byte was not clocked as
    // part of the completed transaction.
    uint32_t transmitted = removed > 0 ? removed - 1 : 0;
    i2c_slave->tx_data_count = 0;
    i2c_ll_txfifo_rst(hal->dev);
    portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);

    if (i2c_slave->transmit_done_callback) {
        i2c_slave_transmit_done_event_data_t edata = {
            .length = transmitted,
        };
        xTaskWoken |= i2c_slave->transmit_done_callback(i2c_slave, &edata, i2c_slave->user_ctx);
    }
    return xTaskWoken;
}
#endif

IRAM_ATTR static bool i2c_slave_handle_rx_fifo(i2c_slave_dev_t *i2c_slave, uint32_t len)
{
    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    uint8_t data[SOC_I2C_FIFO_LEN];
    BaseType_t xTaskWoken = pdFALSE;
    if (len) {
        portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
        i2c_ll_read_rxfifo(hal->dev, data, len);
        portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);
        BaseType_t res = xRingbufferSendFromISR(i2c_slave->rx_ring_buf, (void *)data, len, &xTaskWoken);
        if (res == pdTRUE) {
            i2c_slave->rx_data_count += len;
        } else {
            i2c_slave->receive_overflow = true;
        }
    }
    return xTaskWoken;
}

// The caller must hold the bus spinlock. Keeping the receive/return pair under
// that lock prevents the ISR and task paths from having overlapping byte-buffer
// receives, which the ringbuffer API does not support.
static size_t i2c_slave_fill_tx_fifo(i2c_slave_dev_t *i2c_slave, size_t fifo_len)
{
    size_t loaded = 0;
    while (fifo_len > 0) {
        size_t actual_get_len = 0;
        uint8_t *data = xRingbufferReceiveUpTo(i2c_slave->tx_ring_buf, &actual_get_len, 0, fifo_len);
        if (!data) {
            break;
        }
        i2c_ll_write_txfifo(i2c_slave->base->hal.dev, data, actual_get_len);
        fifo_len -= actual_get_len;
        loaded += actual_get_len;
        vRingbufferReturnItem(i2c_slave->tx_ring_buf, data);
    }
    return loaded;
}

#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
IRAM_ATTR static bool i2c_slave_handle_stretch_event(i2c_slave_dev_t *i2c_slave, uint32_t rx_fifo_exist_len)
{
    i2c_slave_stretch_cause_t cause;
    BaseType_t xTaskWoken = pdFALSE;
    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    i2c_ll_slave_get_stretch_cause(hal->dev, &cause);
    if (cause == I2C_SLAVE_STRETCH_CAUSE_ADDRESS_MATCH) {
        portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
        i2c_slave->request_pending = true;
        portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);
        if (rx_fifo_exist_len) {
            xTaskWoken |= i2c_slave_handle_rx_fifo(i2c_slave, rx_fifo_exist_len);
        }
        if (i2c_slave->rx_data_count || i2c_slave->receive_overflow) {
            uint32_t len = i2c_slave->rx_data_count;
            size_t read_len;
            xTaskWoken |= i2c_slave_read_rx(i2c_slave, i2c_slave->receive_desc.buffer, len, &read_len);
            i2c_slave_rx_done_event_data_t edata = {};
            edata.buffer = i2c_slave->receive_desc.buffer;
            edata.length = read_len;
            edata.overflow = i2c_slave->receive_overflow;
            if (i2c_slave->receive_callback) {
                xTaskWoken |= i2c_slave->receive_callback(i2c_slave, &edata, i2c_slave->user_ctx);
            }
            i2c_slave->rx_data_count = 0;
            i2c_slave->receive_overflow = false;
        }
        i2c_slave_request_event_data_t evt_data = {};
        if (i2c_slave->request_callback) {
            xTaskWoken |= i2c_slave->request_callback(i2c_slave, &evt_data, i2c_slave->user_ctx);
        }
        // Data may have been queued before the request. Move it into the FIFO
        // and release the address-match stretch without requiring another
        // i2c_slave_write call. If no data is available, keep stretching so the
        // request callback can wake a task that produces the response.
        size_t loaded = 0;
        xTaskWoken |= i2c_slave_handle_tx_fifo(i2c_slave, &loaded);
        if (loaded != 0) {
            i2c_ll_slave_enable_tx_it(hal->dev);
            portENTER_CRITICAL_ISR(&i2c_slave->base->spinlock);
            i2c_slave->request_pending = false;
            portEXIT_CRITICAL_ISR(&i2c_slave->base->spinlock);
            i2c_ll_slave_clear_stretch(hal->dev);
        }
    } else if (cause == I2C_SLAVE_STRETCH_CAUSE_TX_EMPTY) {
        xTaskWoken |= i2c_slave_handle_tx_fifo(i2c_slave, NULL);
        i2c_ll_slave_clear_stretch(hal->dev);
    } else if (cause == I2C_SLAVE_STRETCH_CAUSE_RX_FULL) {
        xTaskWoken |= i2c_slave_handle_rx_fifo(i2c_slave, rx_fifo_exist_len);
        i2c_ll_slave_clear_stretch(hal->dev);
    }
    return xTaskWoken;
}
#endif

IRAM_ATTR static void i2c_slave_isr_handler(void *arg)
{
    BaseType_t pxHigherPriorityTaskWoken = false;
    i2c_slave_dev_t *i2c_slave = (i2c_slave_dev_t *)arg;

    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    uint32_t int_mask = 0;
    i2c_ll_get_intr_mask(hal->dev, &int_mask);
    i2c_ll_clear_intr_mask(hal->dev, int_mask);
    uint32_t rx_fifo_exist_len = 0;
    i2c_ll_get_rxfifo_cnt(hal->dev, &rx_fifo_exist_len);
    i2c_slave_read_write_status_t slave_rw = i2c_ll_slave_get_read_write_status(hal->dev);
    bool transmit_done = false;

    if (int_mask & I2C_INTR_SLV_RXFIFO_WM) {
        pxHigherPriorityTaskWoken |= i2c_slave_handle_rx_fifo(i2c_slave, rx_fifo_exist_len);
    }

    if (int_mask & I2C_INTR_SLV_COMPLETE) {
#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
        if (slave_rw == I2C_SLAVE_READ_BY_MASTER) {
            pxHigherPriorityTaskWoken |= i2c_slave_handle_tx_done(i2c_slave);
            transmit_done = i2c_slave->transmit_callback != NULL;
        }
#endif
        if (rx_fifo_exist_len) {
            pxHigherPriorityTaskWoken |= i2c_slave_handle_rx_fifo(i2c_slave, rx_fifo_exist_len);
        }
        if (i2c_slave->rx_data_count || i2c_slave->receive_overflow) {
            uint32_t len = i2c_slave->rx_data_count;
            size_t read_len;
            pxHigherPriorityTaskWoken |= i2c_slave_read_rx(i2c_slave, i2c_slave->receive_desc.buffer, len, &read_len);
            i2c_slave_rx_done_event_data_t edata = {};
            edata.buffer = i2c_slave->receive_desc.buffer;
            edata.length = read_len;
            edata.overflow = i2c_slave->receive_overflow;
            if (i2c_slave->receive_callback) {
                pxHigherPriorityTaskWoken |= i2c_slave->receive_callback(i2c_slave, &edata, i2c_slave->user_ctx);
            }
            i2c_slave->rx_data_count = 0;
            i2c_slave->receive_overflow = false;
        }
        if (slave_rw == I2C_SLAVE_READ_BY_MASTER) {
#if !SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
            i2c_slave_request_event_data_t evt_data = {};
            if (i2c_slave->request_callback && i2c_ll_slave_wait_ack(hal->dev)) {
                pxHigherPriorityTaskWoken |= i2c_slave->request_callback(i2c_slave, &evt_data, i2c_slave->user_ctx);
            }
#endif
        }
    }

#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
    if (int_mask & I2C_INTR_STRETCH) {  // STRETCH
        pxHigherPriorityTaskWoken |= i2c_slave_handle_stretch_event(i2c_slave, rx_fifo_exist_len);
    }
#endif

    if ((int_mask & I2C_INTR_SLV_TXFIFO_WM) && !transmit_done) {  // TX FiFo Empty
        pxHigherPriorityTaskWoken |= i2c_slave_handle_tx_fifo(i2c_slave, NULL);
    }

    if (pxHigherPriorityTaskWoken) {
        portYIELD_FROM_ISR();
    }
}

static esp_err_t i2c_slave_device_destroy(i2c_slave_dev_handle_t i2c_slave)
{
    esp_err_t ret = ESP_OK;
    if (i2c_slave->base) {
        i2c_ll_disable_intr_mask(i2c_slave->base->hal.dev, I2C_LL_SLAVE_EVENT_INTR);
        i2c_common_deinit_pins(i2c_slave->base);
        ret = i2c_release_bus_handle(i2c_slave->base);
    }
    if (i2c_slave->rx_ring_buf) {
        vRingbufferDeleteWithCaps(i2c_slave->rx_ring_buf);
        i2c_slave->rx_ring_buf = NULL;
    }
    if (i2c_slave->tx_ring_buf) {
        vRingbufferDeleteWithCaps(i2c_slave->tx_ring_buf);
        i2c_slave->tx_ring_buf = NULL;
    }
    if (i2c_slave->operation_mux) {
        vSemaphoreDeleteWithCaps(i2c_slave->operation_mux);
        i2c_slave->operation_mux = NULL;
    }
    if (i2c_slave->receive_desc.buffer) {
        free(i2c_slave->receive_desc.buffer);
    }

    free(i2c_slave);
    return ret;
}

esp_err_t i2c_new_slave_device(const i2c_slave_config_t *slave_config, i2c_slave_dev_handle_t *ret_handle)
{
#if CONFIG_I2C_ENABLE_DEBUG_LOG
    esp_log_level_set(TAG, ESP_LOG_DEBUG);
#endif
    esp_err_t ret = ESP_OK;
    i2c_slave_dev_t *i2c_slave = NULL;
    ESP_RETURN_ON_FALSE(slave_config && ret_handle, ESP_ERR_INVALID_ARG, TAG, "invalid argument");
    ESP_RETURN_ON_FALSE(GPIO_IS_VALID_GPIO(slave_config->sda_io_num) && GPIO_IS_VALID_GPIO(slave_config->scl_io_num), ESP_ERR_INVALID_ARG, TAG, "invalid SDA/SCL pin number");
#if SOC_LP_I2C_SUPPORTED
    ESP_RETURN_ON_FALSE(slave_config->i2c_port != (SOC_I2C_NUM - 1), ESP_ERR_NOT_SUPPORTED, TAG, "LP i2c is not supported in I2C slave");
#endif
    ESP_RETURN_ON_FALSE(slave_config->i2c_port < SOC_HP_I2C_NUM || slave_config->i2c_port == -1, ESP_ERR_INVALID_ARG, TAG, "invalid i2c port number");
#if SOC_I2C_SLAVE_SUPPORT_BROADCAST
    ESP_RETURN_ON_FALSE(((slave_config->addr_bit_len != I2C_ADDR_BIT_LEN_10) || (!slave_config->flags.broadcast_en)), ESP_ERR_INVALID_STATE, TAG, "10bits address cannot used together with broadcast");
#endif

    i2c_slave = heap_caps_calloc(1, sizeof(i2c_slave_dev_t), I2C_MEM_ALLOC_CAPS);
    ESP_RETURN_ON_FALSE(i2c_slave, ESP_ERR_NO_MEM, TAG, "no memory for i2c slave bus");

    ESP_GOTO_ON_ERROR(i2c_acquire_bus_handle(slave_config->i2c_port, &i2c_slave->base, I2C_BUS_MODE_SLAVE), err, TAG, "I2C bus acquire failed");

    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    i2c_slave->base->scl_num = slave_config->scl_io_num;
    i2c_slave->base->sda_num = slave_config->sda_io_num;
    i2c_slave->base->pull_up_enable = slave_config->flags.enable_internal_pullup;
    i2c_slave->rx_data_count = 0;
    i2c_port_num_t i2c_port_num = i2c_slave->base->port_num;
    ESP_GOTO_ON_ERROR(i2c_common_set_pins(i2c_slave->base), err, TAG, "i2c slave set pins failed");

    i2c_slave->rx_ring_buf = xRingbufferCreateWithCaps(slave_config->receive_buf_depth, RINGBUF_TYPE_BYTEBUF, I2C_MEM_ALLOC_CAPS);
    ESP_GOTO_ON_FALSE(i2c_slave->rx_ring_buf != NULL, ESP_ERR_NO_MEM, err, TAG, "ringbuffer create failed");

    i2c_slave->operation_mux = xSemaphoreCreateBinaryWithCaps(I2C_MEM_ALLOC_CAPS);
    ESP_GOTO_ON_FALSE(i2c_slave->operation_mux, ESP_ERR_NO_MEM, err, TAG, "No memory for binary semaphore");
    xSemaphoreGive(i2c_slave->operation_mux);

    i2c_slave->receive_desc.buffer = heap_caps_calloc(1, slave_config->receive_buf_depth, I2C_MEM_ALLOC_CAPS);
    ESP_GOTO_ON_FALSE(i2c_slave->receive_desc.buffer, ESP_ERR_NO_MEM, err, TAG, "no memory for i2c slave receive internal buffer");
    i2c_slave->receive_desc.rcv_fifo_cnt = slave_config->receive_buf_depth;

    i2c_slave->tx_ring_buf = xRingbufferCreateWithCaps(slave_config->send_buf_depth, RINGBUF_TYPE_BYTEBUF, I2C_MEM_ALLOC_CAPS);
    ESP_GOTO_ON_FALSE(i2c_slave->tx_ring_buf, ESP_ERR_NO_MEM, err, TAG, "no memory for i2c slave transmit ringbuffer");

#if I2C_USE_RETENTION_LINK
    if (slave_config->flags.allow_pd != 0) {
        i2c_create_retention_module(i2c_slave->base);
    }
#endif // I2C_USE_RETENTION_LINK

    int isr_flags = I2C_INTR_ALLOC_FLAG;
    if (slave_config->intr_priority) {
        isr_flags |= 1 << (slave_config->intr_priority);
    }
    ret = esp_intr_alloc_intrstatus(i2c_periph_signal[i2c_port_num].irq, isr_flags, (uint32_t)i2c_ll_get_interrupt_status_reg(hal->dev), I2C_LL_SLAVE_EVENT_INTR, i2c_slave_isr_handler, i2c_slave, &i2c_slave->base->intr_handle);
    ESP_GOTO_ON_ERROR(ret, err, TAG, "install i2c slave interrupt failed");

    portENTER_CRITICAL(&i2c_slave->base->spinlock);
    i2c_hal_slave_init(hal);
    i2c_ll_enable_fifo_mode(hal->dev, true);
    i2c_ll_set_slave_addr(hal->dev, slave_config->slave_addr, false);
    i2c_ll_set_tout(hal->dev, I2C_LL_MAX_TIMEOUT);

    I2C_CLOCK_SRC_ATOMIC() {
        i2c_ll_set_source_clk(hal->dev, slave_config->clk_source);
    }
    bool addr_10bit_en = slave_config->addr_bit_len != I2C_ADDR_BIT_LEN_7;
    i2c_ll_set_slave_addr(hal->dev, slave_config->slave_addr, addr_10bit_en);

#if SOC_I2C_SLAVE_SUPPORT_BROADCAST
    i2c_ll_slave_broadcast_enable(hal->dev, slave_config->flags.broadcast_en);
#endif

    i2c_ll_set_txfifo_empty_thr(hal->dev, SOC_I2C_FIFO_LEN / 2);
    i2c_ll_set_rxfifo_full_thr(hal->dev, SOC_I2C_FIFO_LEN / 2);
    i2c_ll_set_sda_timing(hal->dev, 10, 10);

#if CONFIG_IDF_TARGET_ESP32S3 || CONFIG_IDF_TARGET_ESP32C3
    // Match upstream ESP-IDF initialization for ESP32-S3 and ESP32-C3.
    // Espressif identifies SLV_TX_AUTO_START_EN as a hardware workaround.
    // See upstream commit 8b8b5df1414d.
    i2c_ll_slave_enable_auto_start(hal->dev, true);
#endif

    i2c_ll_disable_intr_mask(hal->dev, I2C_LL_INTR_MASK);
    i2c_ll_clear_intr_mask(hal->dev, I2C_LL_INTR_MASK);

    i2c_ll_enable_intr_mask(hal->dev, I2C_LL_SLAVE_RX_EVENT_INTR);

    // Configure stretch
    i2c_ll_slave_set_stretch_protect_num(hal->dev, I2C_LL_STRETCH_PROTECT_TIME);
    i2c_ll_slave_enable_scl_stretch(hal->dev, true);
    i2c_ll_slave_clear_stretch(hal->dev);

    i2c_ll_update(hal->dev);
    portEXIT_CRITICAL(&i2c_slave->base->spinlock);

    *ret_handle = i2c_slave;
    return ret;

err:
    if (i2c_slave) {
        i2c_slave_device_destroy(i2c_slave);
    }
    return ret;
}

esp_err_t i2c_del_slave_device(i2c_slave_dev_handle_t i2c_slave)
{
    ESP_RETURN_ON_FALSE(i2c_slave, ESP_ERR_INVALID_ARG, TAG, "i2c slave not initialized");
    int port_id = i2c_slave->base->port_num;
    ESP_LOGD(TAG, "del i2c bus(%d)", port_id);
    ESP_RETURN_ON_ERROR(i2c_slave_device_destroy(i2c_slave), TAG, "destroy i2c bus failed");
    return ESP_OK;
}

esp_err_t i2c_slave_write(i2c_slave_dev_handle_t i2c_slave, const uint8_t *data, uint32_t len, uint32_t *write_len, int timeout_ms)
{
    ESP_RETURN_ON_FALSE(i2c_slave, ESP_ERR_INVALID_ARG, TAG, "i2c slave not initialized");
    ESP_RETURN_ON_FALSE(data, ESP_ERR_INVALID_ARG, TAG, "invalid data buffer");
    ESP_RETURN_ON_FALSE(write_len, ESP_ERR_INVALID_ARG, TAG, "invalid write length pointer");
    ESP_RETURN_ON_FALSE(!i2c_slave->transmit_callback, ESP_ERR_INVALID_STATE, TAG, "transmit callback is registered");
    uint32_t free_fifo_len = 0;
    uint32_t write_ringbuffer_len = 0;
    uint32_t actual_write_fifo_size = 0;
    i2c_hal_context_t *hal = &i2c_slave->base->hal;
    TickType_t wait_ticks = (timeout_ms == -1) ? portMAX_DELAY : pdMS_TO_TICKS(timeout_ms);
    bool request_pending = true;
    bool release_request = false;

    if (xSemaphoreTake(i2c_slave->operation_mux, wait_ticks) != pdTRUE) {
        return ESP_ERR_TIMEOUT;
    }

    portENTER_CRITICAL(&i2c_slave->base->spinlock);
#if !SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
    i2c_ll_slave_disable_tx_it(hal->dev);
    uint32_t txfifo_len = 0;
    i2c_ll_get_txfifo_len(hal->dev, &txfifo_len);
    if (txfifo_len < SOC_I2C_FIFO_LEN) {
        // For the target (esp32) cannot stretch, reset the fifo when there is any dirty data in fifo.
        i2c_ll_txfifo_rst(hal->dev);
    }
#else
    request_pending = i2c_slave->request_pending;
#endif
    if (request_pending) {
        i2c_ll_get_txfifo_len(hal->dev, &free_fifo_len);
    }

    // Check if there is any data in the ringbuffer from the last transaction.
    size_t existing_size = i2c_slave_fill_tx_fifo(i2c_slave, free_fifo_len);
    free_fifo_len -= existing_size;

    // Write data.
    if (free_fifo_len) {
        actual_write_fifo_size = len < free_fifo_len ? len : free_fifo_len;
        i2c_ll_write_txfifo(hal->dev, (uint8_t *)data, actual_write_fifo_size);
        data += actual_write_fifo_size;
        len -= actual_write_fifo_size;
    }
    release_request = request_pending && (existing_size + actual_write_fifo_size) != 0;
#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
    if (release_request) {
        i2c_slave->request_pending = false;
    }
#endif
    portEXIT_CRITICAL(&i2c_slave->base->spinlock);

    if (len) {
        write_ringbuffer_len = xRingbufferGetCurFreeSize(i2c_slave->tx_ring_buf);
        if (len < write_ringbuffer_len) {
            write_ringbuffer_len = len;
        }

        if (xRingbufferSend(i2c_slave->tx_ring_buf, data, write_ringbuffer_len, wait_ticks) != pdTRUE) {
            write_ringbuffer_len = 0;
        }
    }

    *write_len = write_ringbuffer_len + actual_write_fifo_size;

#if SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
    // An address match can race with the ringbuffer send above. If the ISR saw
    // the empty buffer, finish servicing the request here after the data has
    // become visible. The ISR can always run while operation_mux is held.
    if (!release_request && write_ringbuffer_len != 0) {
        portENTER_CRITICAL(&i2c_slave->base->spinlock);
        if (i2c_slave->request_pending) {
            i2c_ll_get_txfifo_len(hal->dev, &free_fifo_len);
            size_t loaded = i2c_slave_fill_tx_fifo(i2c_slave, free_fifo_len);
            if (loaded != 0) {
                i2c_slave->request_pending = false;
                release_request = true;
            }
        }
        portEXIT_CRITICAL(&i2c_slave->base->spinlock);
    }
#endif

    xSemaphoreGive(i2c_slave->operation_mux);
    if (release_request) {
        i2c_ll_slave_enable_tx_it(hal->dev);
        i2c_ll_slave_clear_stretch(hal->dev);
    }

    return ESP_OK;
}

esp_err_t i2c_slave_register_event_callbacks(i2c_slave_dev_handle_t i2c_slave, const i2c_slave_event_callbacks_t *cbs, void *user_data)
{
    ESP_RETURN_ON_FALSE(i2c_slave != NULL, ESP_ERR_INVALID_ARG, TAG, "i2c slave handle not initialized");
    ESP_RETURN_ON_FALSE(cbs, ESP_ERR_INVALID_ARG, TAG, "invalid argument");
#if !SOC_I2C_SLAVE_CAN_GET_STRETCH_CAUSE
    ESP_RETURN_ON_FALSE(!cbs->on_transmit && !cbs->on_transmit_done, ESP_ERR_NOT_SUPPORTED, TAG, "synchronous transmit callbacks are not supported");
#endif
#if CONFIG_I2C_ISR_IRAM_SAFE
    if (cbs->on_request) {
        ESP_RETURN_ON_FALSE(esp_ptr_in_iram(cbs->on_request) || esp_ptr_in_rtc_iram_fast(cbs->on_request), ESP_ERR_INVALID_ARG, TAG, "i2c request occur callback not in IRAM");
    }
    if (cbs->on_receive) {
        ESP_RETURN_ON_FALSE(esp_ptr_in_iram(cbs->on_receive) || esp_ptr_in_rtc_iram_fast(cbs->on_receive), ESP_ERR_INVALID_ARG, TAG, "i2c receive occur callback not in IRAM");
    }
    if (cbs->on_transmit) {
        ESP_RETURN_ON_FALSE(esp_ptr_in_iram(cbs->on_transmit) || esp_ptr_in_rtc_iram_fast(cbs->on_transmit), ESP_ERR_INVALID_ARG, TAG, "i2c transmit callback not in IRAM");
    }
    if (cbs->on_transmit_done) {
        ESP_RETURN_ON_FALSE(esp_ptr_in_iram(cbs->on_transmit_done) || esp_ptr_in_rtc_iram_fast(cbs->on_transmit_done), ESP_ERR_INVALID_ARG, TAG, "i2c transmit done callback not in IRAM");
    }
    if (user_data) {
        ESP_RETURN_ON_FALSE(esp_ptr_internal(user_data), ESP_ERR_INVALID_ARG, TAG, "user context not in internal RAM");
    }
#endif

    i2c_slave->user_ctx = user_data;
    i2c_slave->request_callback = cbs->on_request;
    i2c_slave->receive_callback = cbs->on_receive;
    i2c_slave->transmit_callback = cbs->on_transmit;
    i2c_slave->transmit_done_callback = cbs->on_transmit_done;
    return ESP_OK;
}
