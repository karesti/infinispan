package org.infinispan.spring.remote.session;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.springframework.session.MapSession;

@AutoProtoSchemaBuilder(
      includeClasses = {
            MapSession.class
      },
      schemaFileName = "mapsession.proto",
      schemaFilePath = "proto/",
      schemaPackageName = "spring_integration")
interface MapSessionProto extends SerializationContextInitializer {
}
