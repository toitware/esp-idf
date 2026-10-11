// Copyright (C) 2026 Toit contributors.
// SPDX-License-Identifier: Apache-2.0

#include <stdlib.h>
#include <string.h>
#include "driver/i2s_std.h"
#include "esp_rom_gpio.h"
#include "soc/gpio_sig_map.h"
#include "soc/i2s_periph.h"
#include "unity.h"
#include "i2s_private.h"

/* Hold the slave clocks low so there are no EOFs and the free queue cannot
 * refill. No GPIO is driven. This makes partial writes and timeouts deterministic
 * without assuming a particular interrupt latency or requiring loopback wiring. */
TEST_CASE("I2S TX timeout preserves an in-flight tail", "[i2s]")
{
    for (unsigned clear_mode = 0; clear_mode < 3; clear_mode++) {
        i2s_chan_config_t cfg = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_AUTO, I2S_ROLE_SLAVE);
        cfg.dma_desc_num = 2;
        cfg.dma_frame_num = 240;
        cfg.auto_clear_before_cb = clear_mode == 1;
        cfg.auto_clear_after_cb = clear_mode == 2;
        i2s_chan_handle_t tx = NULL;
        TEST_ESP_OK(i2s_new_channel(&cfg, &tx, NULL));
        i2s_std_config_t std = {
            .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(10000),
            .slot_cfg = I2S_STD_MSB_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_STEREO),
            .gpio_cfg = {
                .mclk = I2S_GPIO_UNUSED, .bclk = I2S_GPIO_UNUSED,
                .ws = I2S_GPIO_UNUSED, .dout = I2S_GPIO_UNUSED, .din = I2S_GPIO_UNUSED,
            },
        };
        TEST_ESP_OK(i2s_channel_init_std_mode(tx, &std));
        int port = tx->controller->id;
        esp_rom_gpio_connect_in_signal(GPIO_MATRIX_CONST_ZERO_INPUT, i2s_periph_signal[port].s_tx_bck_sig, false);
        esp_rom_gpio_connect_in_signal(GPIO_MATRIX_CONST_ZERO_INPUT, i2s_periph_signal[port].s_tx_ws_sig, false);

        size_t size = tx->dma.buf_size;
        uint8_t *old = malloc(2 * size);
        uint8_t *next = malloc(size + 4);
        TEST_ASSERT_NOT_NULL(old);
        TEST_ASSERT_NOT_NULL(next);
        memset(old, 0xa5, 2 * size);
        memset(next, 0x3c, size + 4);
        size_t loaded = 0;
        TEST_ESP_OK(i2s_channel_preload_data(tx, old, 2 * size, &loaded));
        TEST_ASSERT_EQUAL_UINT32(2 * size, loaded);
        TEST_ESP_OK(i2s_channel_enable(tx));
        TEST_ESP_OK(i2s_channel_disable(tx));

        /* Reset retains DMA storage. Partially preload descriptor 0 again;
         * its unwritten tail represents data from an earlier ring pass. */
        TEST_ESP_OK(i2s_channel_preload_data(tx, next, 4, &loaded));
        TEST_ASSERT_EQUAL_UINT32(4, loaded);
        TEST_ESP_OK(i2s_channel_enable(tx));

        size_t written = SIZE_MAX;
        TEST_ASSERT_EQUAL(ESP_ERR_TIMEOUT, i2s_channel_write(tx, next, size + 4, &written, 0));
        TEST_ASSERT_EQUAL_UINT32(size, written);
        TEST_ASSERT_EQUAL_MEMORY(next, tx->dma.desc[0]->buf, 4);
        TEST_ASSERT_EQUAL_MEMORY(old + 4, (const uint8_t *)tx->dma.desc[0]->buf + 4, size - 4);
        TEST_ASSERT_EQUAL_MEMORY(next, tx->dma.desc[1]->buf, size);

        /* A subsequent timed-out call must reset bytes_written to zero and
         * must not overwrite either buffer. Auto-clear only runs at EOF. */
        written = SIZE_MAX;
        TEST_ASSERT_EQUAL(ESP_ERR_TIMEOUT, i2s_channel_write(tx, old, 4, &written, 20));
        TEST_ASSERT_EQUAL_UINT32(0, written);
        TEST_ASSERT_EQUAL_MEMORY(next, tx->dma.desc[0]->buf, 4);
        TEST_ASSERT_EQUAL_MEMORY(old + 4, (const uint8_t *)tx->dma.desc[0]->buf + 4, size - 4);
        TEST_ASSERT_EQUAL_MEMORY(next, tx->dma.desc[1]->buf, size);
        TEST_ESP_OK(i2s_channel_disable(tx));
        TEST_ESP_OK(i2s_del_channel(tx));
        free(next);
        free(old);
        printf("TX timeout accounting and untouched tail: clear_mode=%u PASS\n", clear_mode);
    }
}
