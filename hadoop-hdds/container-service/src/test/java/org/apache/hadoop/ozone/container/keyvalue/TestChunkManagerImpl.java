/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.helpers.ChunkUtils;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeIOStats;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerDummyImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * This class is used to test ChunkManager operations.
 */
public class TestChunkManagerImpl {

  private OzoneConfiguration config;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private HddsVolume hddsVolume;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private BlockID blockID;
  private ChunkManagerImpl chunkManager;
  private ChunkInfo chunkInfo;
  private ByteBuffer data;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private byte[] header;

  @Before
  public void setUp() throws Exception {
    config = new OzoneConfiguration();
    UUID datanodeId = UUID.randomUUID();
    hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(config).datanodeUuid(datanodeId
        .toString()).build();

    volumeSet = mock(VolumeSet.class);

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());

    keyValueContainer = new KeyValueContainer(keyValueContainerData, config);

    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    header = "my header".getBytes(UTF_8);
    byte[] bytes = "testing write chunks".getBytes(UTF_8);
    data = ByteBuffer.allocate(header.length + bytes.length)
        .put(header).put(bytes);
    rewindBufferToDataStart();

    // Creating BlockData
    blockID = new BlockID(1L, 1L);
    chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
        .getLocalID(), 0), 0, bytes.length);

    // Create a ChunkManager object.
    chunkManager = new ChunkManagerImpl(true);
  }

  private DispatcherContext getDispatcherContext() {
    return new DispatcherContext.Builder().build();
  }

  @Test
  public void testWriteChunkStageWriteAndCommit() throws Exception {
    checkChunkFileCount(0);

    // As no chunks are written to the volume writeBytes should be 0
    checkWriteIOStats(0, 0);
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        new DispatcherContext.Builder()
            .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA).build());
    // Now a chunk file is being written with Stage WRITE_DATA, so it should
    // create a temporary chunk file.
    checkChunkFileCount(1);

    long term = 0;
    long index = 0;
    File chunkFile = ChunkUtils.getChunkFile(keyValueContainerData, chunkInfo);
    File tempChunkFile = new File(chunkFile.getParent(),
        chunkFile.getName() + OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER
            + OzoneConsts.CONTAINER_TEMPORARY_CHUNK_PREFIX
            + OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER + term
            + OzoneConsts.CONTAINER_CHUNK_NAME_DELIMITER + index);

    // As chunk write stage is WRITE_DATA, temp chunk file will be created.
    assertTrue(tempChunkFile.exists());

    checkWriteIOStats(chunkInfo.getLen(), 1);

    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        new DispatcherContext.Builder()
            .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA).build());

    checkWriteIOStats(chunkInfo.getLen(), 1);

    // Old temp file should have been renamed to chunk file.
    checkChunkFileCount(1);

    // As commit happened, chunk file should exist.
    assertTrue(chunkFile.exists());
    assertFalse(tempChunkFile.exists());
  }

  @Test
  public void testWriteChunkIncorrectLength() throws Exception {
    try {
      long randomLength = 200L;
      chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, randomLength);
      chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
          getDispatcherContext());
      fail("testWriteChunkIncorrectLength failed");
    } catch (StorageContainerException ex) {
      // As we got an exception, writeBytes should be 0.
      checkWriteIOStats(0, 0);
      GenericTestUtils.assertExceptionContains("data array does not match " +
          "the length ", ex);
      assertEquals(ContainerProtos.Result.INVALID_WRITE_SIZE, ex.getResult());
    }
  }

  @Test
  public void testWriteChunkStageCombinedData() throws Exception {
    checkChunkFileCount(0);
    checkWriteIOStats(0, 0);
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        getDispatcherContext());
    // Now a chunk file is being written with Stage COMBINED_DATA, so it should
    // create a chunk file.
    checkChunkFileCount(1);
    File chunkFile = ChunkUtils.getChunkFile(keyValueContainerData, chunkInfo);
    assertTrue(chunkFile.exists());
    checkWriteIOStats(chunkInfo.getLen(), 1);
  }

  @Test
  public void testReadChunk() throws Exception {
    checkWriteIOStats(0, 0);
    DispatcherContext dispatcherContext = getDispatcherContext();
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        dispatcherContext);
    checkWriteIOStats(chunkInfo.getLen(), 1);
    checkReadIOStats(0, 0);
    ByteBuffer expectedData = chunkManager
        .readChunk(keyValueContainer, blockID, chunkInfo, dispatcherContext)
        .toByteString().asReadOnlyByteBuffer();
    assertEquals(chunkInfo.getLen(), expectedData.remaining());
    assertEquals(expectedData.rewind(), rewindBufferToDataStart());
    checkReadIOStats(expectedData.limit(), 1);
  }

  @Test
  public void testDeleteChunk() throws Exception {
    chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
        getDispatcherContext());
    checkChunkFileCount(1);

    chunkManager.deleteChunk(keyValueContainer, blockID, chunkInfo);

    checkChunkFileCount(0);
  }

  @Test
  public void testDeleteChunkUnsupportedRequest() throws Exception {
    try {
      chunkManager.writeChunk(keyValueContainer, blockID, chunkInfo, data,
          getDispatcherContext());
      long randomLength = 200L;
      chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, randomLength);
      chunkManager.deleteChunk(keyValueContainer, blockID, chunkInfo);
      fail("testDeleteChunkUnsupportedRequest");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Not Supported Operation.", ex);
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex.getResult());
    }
  }

  @Test
  public void testReadChunkFileNotExists() throws Exception {
    try {
      // trying to read a chunk, where chunk file does not exist
      chunkManager.readChunk(keyValueContainer,
          blockID, chunkInfo, getDispatcherContext());
      fail("testReadChunkFileNotExists failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Chunk file can't be found", ex);
      assertEquals(ContainerProtos.Result.UNABLE_TO_FIND_CHUNK, ex.getResult());
    }
  }

  @Test
  public void testWriteAndReadChunkMultipleTimes() throws Exception {
    for (int i=0; i<100; i++) {
      ChunkInfo info = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), i),
          0, this.chunkInfo.getLen());
      chunkManager.writeChunk(keyValueContainer, blockID, info, data,
          getDispatcherContext());
      rewindBufferToDataStart();
    }
    checkWriteIOStats(chunkInfo.getLen()*100, 100);
    assertTrue(hddsVolume.getVolumeIOStats().getWriteTime() > 0);

    for (int i=0; i<100; i++) {
      ChunkInfo info = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), i),
          0, this.chunkInfo.getLen());
      chunkManager.readChunk(keyValueContainer, blockID, info,
          getDispatcherContext());
    }
    checkReadIOStats(chunkInfo.getLen()*100, 100);
    assertTrue(hddsVolume.getVolumeIOStats().getReadTime() > 0);
  }

  @Test
  public void dummyManagerDoesNotWriteToFile() throws Exception {
    ChunkManager dummy = new ChunkManagerDummyImpl(true);
    DispatcherContext ctx = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA).build();

    dummy.writeChunk(keyValueContainer, blockID, chunkInfo, data, ctx);

    checkChunkFileCount(0);
  }

  @Test
  public void dummyManagerReadsAnyChunk() throws Exception {
    ChunkManager dummy = new ChunkManagerDummyImpl(true);

    ChunkBuffer dataRead = dummy.readChunk(keyValueContainer,
        blockID, chunkInfo, getDispatcherContext());

    assertNotNull(dataRead);
  }

  private Buffer rewindBufferToDataStart() {
    return data.position(header.length);
  }

  private void checkChunkFileCount(int expected) {
    //As in Setup, we try to create container, these paths should exist.
    String path = keyValueContainerData.getChunksPath();
    assertNotNull(path);

    File dir = new File(path);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(expected, files.length);
  }

  /**
   * Check WriteIO stats.
   * @param length
   * @param opCount
   */
  private void checkWriteIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getWriteBytes());
    assertEquals(opCount, volumeIOStats.getWriteOpCount());
  }

  /**
   * Check ReadIO stats.
   * @param length
   * @param opCount
   */
  private void checkReadIOStats(long length, long opCount) {
    VolumeIOStats volumeIOStats = hddsVolume.getVolumeIOStats();
    assertEquals(length, volumeIOStats.getReadBytes());
    assertEquals(opCount, volumeIOStats.getReadOpCount());
  }
}
