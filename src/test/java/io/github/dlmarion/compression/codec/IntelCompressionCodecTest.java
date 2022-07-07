package io.github.dlmarion.compression.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.CompressionAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.compression.hadoop.conf.IntelCompressionCodecConfigurationKeys;

public class IntelCompressionCodecTest {

  private static final Logger LOG = LoggerFactory.getLogger(IntelCompressionCodecTest.class);

  private static final byte[] DATA1 = "Dogs don't know it's not bacon!\n".getBytes(StandardCharsets.UTF_8);
  private static final Configuration CONF = new Configuration(false);
  
  @BeforeClass
  public static void beforeAll() throws Exception {
    // The IntelCompressionCodec supports multiple compression schemes, we need to
    // tell it which one we want to use.
    CONF.set(IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_KEY, "zlib-ipp");
    //CONF.set(IntelCompressionCodecConfigurationKeys.INTEL_COMPRESSION_CODEC_KEY, "igzip");
    LOG.info("Output is {} bytes", DATA1.length);
  }

  @Test
  public void testAlgorithmLoaded() throws Exception {
    assertTrue("IntelCompressionCodec not loaded", Compression.getSupportedAlgorithms().contains("intel"));
  }
  
  @Test
  public void testZlibIppWriteRead() throws Exception {
    LOG.info("Starting testZlibIppWriteRead");
    CompressionAlgorithm gzAlgo = Compression.getCompressionAlgorithmByName("intel");
    Compressor c = gzAlgo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = gzAlgo.createCompressionStream(baos, c, 1024);
    out.write(DATA1);
    out.flush();
    out.close();
    c.finish();
    gzAlgo.returnCompressor(c);
    
    byte[] compressedData = baos.toByteArray();
    LOG.info("{}", compressedData);

    ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
    CompressionAlgorithm intelAlgo = Compression.getCompressionAlgorithmByName("intel");
    Decompressor d = intelAlgo.getDecompressor();
    InputStream in = intelAlgo.createDecompressionStream(bais, d, 1024);
    byte[] buffer = in.readAllBytes();
    LOG.info("read {} bytes", buffer.length);
    intelAlgo.returnDecompressor(d);
    assertArrayEquals("Input must match output", DATA1, buffer);
  }
  
  @Test
  public void testDefaultWriteZlibIppRead() throws Exception {
    LOG.info("Starting testDefaultWriteZlibIppRead");
    CompressionAlgorithm gzAlgo = Compression.getCompressionAlgorithmByName("gz");
    Compressor c = gzAlgo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = gzAlgo.createCompressionStream(baos, c, 1024);
    out.write(DATA1);
    out.flush();
    out.close();
    c.finish();
    gzAlgo.returnCompressor(c);
    
    byte[] compressedData = baos.toByteArray();
    LOG.info("{}", compressedData);

    ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
    CompressionAlgorithm intelAlgo = Compression.getCompressionAlgorithmByName("intel");
    Decompressor d = intelAlgo.getDecompressor();
    InputStream in = intelAlgo.createDecompressionStream(bais, d, 1024);
    byte[] buffer = in.readAllBytes();
    LOG.info("read {} bytes", buffer.length);
    intelAlgo.returnDecompressor(d);
    assertArrayEquals("Input must match output", DATA1, buffer);
  }
  
  @Test
  @Ignore
  public void testDefaultWriteIgzipRead() throws Exception {
    CompressionAlgorithm gzAlgo = Compression.getCompressionAlgorithmByName("gz");
    Compressor c = gzAlgo.getCompressor();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream out = gzAlgo.createCompressionStream(baos, c, 1024);
    out.write(DATA1);
    out.flush();
    out.close();
    c.finish();
    gzAlgo.returnCompressor(c);
    
    byte[] compressedData = baos.toByteArray();
    LOG.info("{}", compressedData);

    ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
    CompressionAlgorithm intelAlgo = Compression.getCompressionAlgorithmByName("intel");
    Decompressor d = intelAlgo.getDecompressor();
    InputStream in = intelAlgo.createDecompressionStream(bais, d, 1024);
    byte[] buffer = in.readAllBytes();
    LOG.info("read {} bytes", buffer.length);
    intelAlgo.returnDecompressor(d);
    assertEquals("Input must match output", DATA1, buffer);
  }

  
}
