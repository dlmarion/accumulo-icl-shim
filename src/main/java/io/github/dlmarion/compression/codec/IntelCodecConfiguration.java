package io.github.dlmarion.compression.codec;

import org.apache.accumulo.core.spi.file.rfile.compression.CompressionAlgorithmConfiguration;

import com.intel.compression.hadoop.conf.IntelCompressionCodecConfigurationKeys;

public class IntelCodecConfiguration implements CompressionAlgorithmConfiguration {

  public String getName() {
    return "intel";
  }

  public String getCodecClassName() {
    return "com.intel.compression.hadoop.IntelCompressionCodec";
  }

  public String getCodecClassNameProperty() {
    return "io.compression.codec.intel.class";
  }

  public int getDefaultBufferSize() {
    return IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_DEFAULT;
  }

  public String getBufferSizeProperty() {
    return IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_BUFFER_SIZE_KEY;
  }


}
