/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.datanode.cli;

import org.apache.hadoop.ozone.container.common.transport.server
    .ratis.ContainerStateMachine;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.tools.ParseRatisLog;
import picocli.CommandLine;

import java.io.File;

/**
 * Command line utility to parse and dump a datanode ratis segment file.
 */
@CommandLine.Command(
    description = "Utility to parse and dump datanode segment file",
    name = "dnRatisparser", mixinStandardHelpOptions = true)
public class ParseDnRatisLogSegment implements Runnable {
  @CommandLine.Option(names = {"-s", "--segmentPath"}, required = true,
      description = "Path of the segment file")
  private static File segmentFile;

  private static String smToContainerLogString(
      StateMachineLogEntryProto logEntryProto) {
    return ContainerStateMachine.
        smProtoToString(RaftGroupId.randomId(), null, logEntryProto);
  }

  public void run() {
    try {
      ParseRatisLog.Builder builder = new ParseRatisLog.Builder();
      builder.setSegmentFile(segmentFile);
      builder.setSMLogToString(ParseDnRatisLogSegment::smToContainerLogString);

      ParseRatisLog prl = builder.build();
      prl.dumpSegmentFile();
    } catch (Exception e) {
      System.out.println(ParseDnRatisLogSegment.class.getSimpleName()
          + "failed with exception  " + e.toString());
    }
  }


  public static void main(String... args) {
    CommandLine.run(new ParseDnRatisLogSegment(), System.err, args);
  }
}
