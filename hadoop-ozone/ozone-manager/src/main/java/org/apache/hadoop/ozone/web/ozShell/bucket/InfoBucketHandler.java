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
package org.apache.hadoop.ozone.web.ozShell.bucket;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.ObjectPrinter;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Executes Info bucket.
 */
@Command(name = "info",
    description = "returns information about a bucket")
public class InfoBucketHandler extends Handler {

  @Parameters(arity = "1..1", description = Shell.OZONE_BUCKET_URI_DESCRIPTION)
  private String uri;

  /**
   * Executes the Client Calls.
   */
  @Override
  public Void call() throws Exception {
    OzoneAddress address = new OzoneAddress(uri);
    address.ensureBucketAddress();
    try (OzoneClient client =
             address.createClient(createOzoneConfiguration())) {

      String volumeName = address.getVolumeName();
      String bucketName = address.getBucketName();

      if (isVerbose()) {
        System.out.printf("Volume Name : %s%n", volumeName);
        System.out.printf("Bucket Name : %s%n", bucketName);
      }

      OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
      OzoneBucket bucket = vol.getBucket(bucketName);

      ObjectPrinter.printObjectAsJson(bucket);
    }

    return null;
  }

}
