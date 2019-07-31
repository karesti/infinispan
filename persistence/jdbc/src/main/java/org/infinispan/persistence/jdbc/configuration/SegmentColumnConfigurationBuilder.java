package org.infinispan.persistence.jdbc.configuration;


import static org.infinispan.persistence.jdbc.configuration.SegmentColumnConfiguration.SEGMENT_COLUMN_NAME;
import static org.infinispan.persistence.jdbc.configuration.SegmentColumnConfiguration.SEGMENT_COLUMN_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class SegmentColumnConfigurationBuilder implements Builder<SegmentColumnConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   SegmentColumnConfigurationBuilder() {
      attributes = SegmentColumnConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return SegmentColumnConfiguration.ELEMENT_DEFINITION;
   }

   public SegmentColumnConfigurationBuilder columnName(String columnName) {
      attributes.attribute(SEGMENT_COLUMN_NAME).set(columnName);
      return this;
   }

   public SegmentColumnConfigurationBuilder columnType(String columnType) {
      attributes.attribute(SEGMENT_COLUMN_TYPE).set(columnType);
      return this;
   }

   @Override
   public void validate() {
      TableManipulationConfigurationBuilder.validateIfSet(attributes, SEGMENT_COLUMN_NAME, SEGMENT_COLUMN_TYPE);
   }

   @Override
   public SegmentColumnConfiguration create() {
      return new SegmentColumnConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(SegmentColumnConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
