/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import { ApmFields, Fields } from '..';

export interface StreamAggregator<TFields extends Fields = ApmFields> {
  name: string;

  getWriteTarget(document: Record<string, any>): string | null;

  process(document: TFields): Fields[];

  bootstrapElasticsearch(): Promise<void>;
}

export class ServiceLatencyGenerator implements StreamAggregator<ApmFields> {
  public readonly name;

  constructor() {
    this.name = 'service-latency';
  }

  getWriteTarget(document: Record<string, any>): string | null {
    if (!document.processor?.event) {
      throw Error("'processor.event' is not set on document, can not determine target index");
    }
    const eventType = document.processor.event;
    if (eventType === 'service') return 'services-apm-default';
    return null;
  }

  process(document: ApmFields): Fields[] {
    throw new Error('Method not implemented.');
  }
  bootstrapElasticsearch(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
