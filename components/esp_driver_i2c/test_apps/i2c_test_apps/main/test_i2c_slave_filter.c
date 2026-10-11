// Copyright (C) 2026 Toit contributors.
// SPDX-FileCopyrightText: 2026 Toit contributors
// SPDX-License-Identifier: Unlicense OR CC0-1.0

#include "sdkconfig.h"

#if CONFIG_IDF_TARGET_ESP32S3
#include "driver/i2c_slave.h"
#include "hal/i2c_ll.h"
#include "test_board.h"
#include "unity.h"

TEST_CASE("I2C slave restores input filters on both S3 controllers", "[i2c][test_env=generic]")
{
    for (int cycle = 0; cycle < 3; cycle++) {
        for (int port = 0; port < SOC_HP_I2C_NUM; port++) {
            i2c_slave_config_t config = {
                .i2c_port = port,
                .clk_source = I2C_CLK_SRC_DEFAULT,
                .sda_io_num = I2C_SLAVE_SDA_IO,
                .scl_io_num = I2C_SLAVE_SCL_IO,
                .slave_addr = ESP_SLAVE_ADDR,
                .send_buf_depth = DATA_LENGTH,
                .receive_buf_depth = DATA_LENGTH,
                .flags.enable_internal_pullup = true,
            };
            i2c_slave_dev_handle_t handle;
            TEST_ESP_OK(i2c_new_slave_device(&config, &handle));
            i2c_dev_t *hw = I2C_LL_GET_HW(port);
            bool scl_enabled = hw->filter_cfg.scl_filter_en;
            bool sda_enabled = hw->filter_cfg.sda_filter_en;
            unsigned scl_threshold = hw->filter_cfg.scl_filter_thres;
            unsigned sda_threshold = hw->filter_cfg.sda_filter_thres;

            // Leave a different setting behind before reopening the controller.
            // This test does not generate traffic or drive either signal low.
            i2c_ll_master_set_filter(hw, 1);
            i2c_ll_update(hw);
            TEST_ESP_OK(i2c_del_slave_device(handle));

            // Release the device before asserting so a failed check cannot
            // leave a controller allocated for the following test case.
            TEST_ASSERT_TRUE(scl_enabled);
            TEST_ASSERT_TRUE(sda_enabled);
            TEST_ASSERT_EQUAL_UINT(7, scl_threshold);
            TEST_ASSERT_EQUAL_UINT(7, sda_threshold);
        }
    }
}
#endif
