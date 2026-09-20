| Supported Targets | ESP32 | ESP32-C2 | ESP32-C3 | ESP32-C6 | ESP32-H2 | ESP32-P4 | ESP32-S2 | ESP32-S3 |
| ----------------- | ----- | -------- | -------- | -------- | -------- | -------- | -------- | -------- |

The classic ESP32 RX FIFO count regression uses the `slave_v2` configuration
and two devices wired together on the standard test pins, GPIO18 for SCL and
GPIO19 for SDA, with a common ground. Build the test application and run only
that pytest configuration with:

```sh
idf.py -B build_esp32_slave_v2 -D IDF_TARGET=esp32 \
  -D SDKCONFIG=build_esp32_slave_v2/sdkconfig \
  -D SDKCONFIG_DEFAULTS='sdkconfig.defaults;sdkconfig.ci.slave_v2' build
pytest pytest_i2c.py::test_i2c_multi_device --target esp32 -k slave_v2 \
  --build-dir build_esp32_slave_v2 --port '/dev/ttyUSB0|/dev/ttyUSB1'
```

The Unity case is `I2C slave refreshes RX FIFO count between interrupt causes`.

The case holds only the target I2C interrupt disabled until the controller has
written 20 bytes and sent STOP. This exceeds the 16-byte RX watermark while
fitting inside the 32-byte FIFO. It checks the pending watermark/completion
interrupts and FIFO count, then re-enables the interrupt and asserts the exact
receive length and contents with no overflow, for three distinct payloads.
The unpatched driver reports 40 bytes and fails the length assertion.
