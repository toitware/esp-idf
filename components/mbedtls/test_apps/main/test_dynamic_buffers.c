// Copyright (C) 2026 Toit contributors.
// SPDX-FileCopyrightText: 2026 Toit contributors
// SPDX-License-Identifier: Unlicense OR CC0-1.0

#include "sdkconfig.h"

#if CONFIG_MBEDTLS_DYNAMIC_BUFFER

#include "esp_mbedtls_dynamic_impl.h"
#include "unity.h"

typedef struct {
    unsigned char header[5];
    size_t available;
    size_t consumed;
    int pause_result;
} header_input_t;

static int receive_header(void *context, unsigned char *buffer, size_t length)
{
    header_input_t *input = context;
    size_t available = input->available - input->consumed;
    if (available == 0) {
        return input->pause_result;
    }
    if (length > available) {
        length = available;
    }
    memcpy(buffer, input->header + input->consumed, length);
    input->consumed += length;
    return length;
}

static void init_receiver(mbedtls_ssl_context *ssl, mbedtls_ssl_config *config, header_input_t *input)
{
    mbedtls_ssl_init(ssl);
    ssl->MBEDTLS_PRIVATE(conf) = config;
    mbedtls_ssl_set_bio(ssl, input, NULL, receive_header, NULL);
    esp_mbedtls_setup_rx_buffer(ssl);
}

static void check_header(mbedtls_ssl_context *ssl, const header_input_t *input)
{
    TEST_ASSERT_EQUAL(5, ssl->MBEDTLS_PRIVATE(in_left));
    TEST_ASSERT_EQUAL_MEMORY(input->header, ssl->MBEDTLS_PRIVATE(in_hdr), 5);
}

TEST_CASE("mbedtls dynamic RX preserves split headers across connections", "[mbedtls][dynamic_buffers]")
{
    mbedtls_ssl_config config;
    mbedtls_ssl_config_init(&config);

    for (size_t split = 0; split < 5; ++split) {
        mbedtls_ssl_context first, second;
        header_input_t a = { .header = {23, 3, 3, 0, 64}, .available = split,
                             .pause_result = MBEDTLS_ERR_SSL_WANT_READ };
        header_input_t b = { .header = {21, 3, 3, 0, 32}, .available = split,
                             .pause_result = MBEDTLS_ERR_SSL_WANT_READ };
        init_receiver(&first, &config, &a);
        init_receiver(&second, &config, &b);

        /* Cover both the first header and the compact buffer between records. */
        for (int record = 0; record < 2; ++record) {
            TEST_ASSERT_EQUAL(MBEDTLS_ERR_SSL_WANT_READ, esp_mbedtls_add_rx_buffer(&first));
            TEST_ASSERT_EQUAL(MBEDTLS_ERR_SSL_WANT_READ, esp_mbedtls_add_rx_buffer(&second));
            TEST_ASSERT_EQUAL(MBEDTLS_ERR_SSL_WANT_READ, esp_mbedtls_add_rx_buffer(&first));
            TEST_ASSERT_EQUAL(MBEDTLS_ERR_SSL_WANT_READ, esp_mbedtls_add_rx_buffer(&second));

            b.available = 5;
            TEST_ASSERT_EQUAL(0, esp_mbedtls_add_rx_buffer(&second));
            check_header(&second, &b);
            a.available = 5;
            TEST_ASSERT_EQUAL(0, esp_mbedtls_add_rx_buffer(&first));
            check_header(&first, &a);

            /* An already allocated record must not fetch another header. */
            TEST_ASSERT_EQUAL(0, esp_mbedtls_add_rx_buffer(&first));
            check_header(&first, &a);

            if (record == 1) {
                const unsigned char counter[8] = {1, 2, 3, 4, 5, 6, 7, 8};
                const unsigned char iv[8] = {9, 10, 11, 12, 13, 14, 15, 16};
                TEST_ASSERT_EQUAL_MEMORY(counter, first.MBEDTLS_PRIVATE(in_ctr), 8);
                TEST_ASSERT_EQUAL_MEMORY(iv, first.MBEDTLS_PRIVATE(in_iv), 8);
            } else {
                for (int i = 0; i < 8; ++i) {
                    first.MBEDTLS_PRIVATE(in_ctr)[i] = i + 1;
                    first.MBEDTLS_PRIVATE(in_iv)[i] = i + 9;
                }
                first.MBEDTLS_PRIVATE(in_msgtype) = 23;
                second.MBEDTLS_PRIVATE(in_msgtype) = 21;
                TEST_ASSERT_EQUAL(0, esp_mbedtls_free_rx_buffer(&first));
                TEST_ASSERT_EQUAL(0, esp_mbedtls_free_rx_buffer(&second));
                a.available = b.available = split;
                a.consumed = b.consumed = 0;
                a.header[4] = 80;
                b.header[4] = 48;
            }
        }
        mbedtls_ssl_free(&first);
        mbedtls_ssl_free(&second);
    }
    mbedtls_ssl_config_free(&config);
}

TEST_CASE("mbedtls dynamic RX can close or reset with a partial header", "[mbedtls][dynamic_buffers]")
{
    mbedtls_ssl_config config;
    mbedtls_ssl_config_init(&config);
    const int results[] = {MBEDTLS_ERR_SSL_WANT_READ, MBEDTLS_ERR_SSL_TIMEOUT, 0};
    for (size_t i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        mbedtls_ssl_context ssl;
        header_input_t input = { .header = {23, 3, 3, 0, 64}, .available = 2,
                                 .pause_result = results[i] };
        init_receiver(&ssl, &config, &input);
        int expected = results[i] == 0 ? MBEDTLS_ERR_SSL_CONN_EOF : results[i];
        TEST_ASSERT_EQUAL(expected, esp_mbedtls_add_rx_buffer(&ssl));
        /* Exercise the same buffer replacement used by session reset. */
        TEST_ASSERT_EQUAL(0, esp_mbedtls_reset_add_rx_buffer(&ssl));
        esp_mbedtls_reset_free_rx_buffer(&ssl);
        input.consumed = 0;
        input.available = 5;
        TEST_ASSERT_EQUAL(0, esp_mbedtls_add_rx_buffer(&ssl));
        check_header(&ssl, &input);
        mbedtls_ssl_free(&ssl);

        input.consumed = 0;
        input.available = 2;
        init_receiver(&ssl, &config, &input);
        TEST_ASSERT_EQUAL(expected, esp_mbedtls_add_rx_buffer(&ssl));
        mbedtls_ssl_free(&ssl);
    }
    mbedtls_ssl_config_free(&config);
}

#endif /* CONFIG_MBEDTLS_DYNAMIC_BUFFER */
