/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import { Client } from '@elastic/elasticsearch';
import { ApmFields, Fields } from '..';
import { Observer } from './apm/apm_fields';
interface LatencyState {
  count: number;
  mean: number;
}

export type ServiceFields = Fields &
  Partial<{
    'timestamp.us'?: number;
    'ecs.version': string;
    'metricset.name': string;
    observer: Observer;
    'processor.event': string;
    'processor.name': string;
    'service.name': string;
    'service.version': string;
    'service.environment': string;
  }>;

export interface StreamAggregator<TFields extends Fields = ApmFields> {
  name: string;

  getWriteTarget(document: Record<string, any>): string | null;

  process(event: TFields): Fields[] | null;

  flush(): Fields[];

  bootstrapElasticsearch(esClient: Client): Promise<void>;

  getDataStreamName(): string;

  getDimensions(): string[];

  getMappings(): Record<string, any>;
}

export class ServiceLatencyGenerator implements StreamAggregator<ApmFields> {
  public readonly name;

  constructor() {
    this.name = 'service-latency';
  }

  getDataStreamName(): string {
    return 'metrics-apm.service';
  }
  getMappings(): Record<string, any> {
    return {
      properties: {
        '@timestamp': {
          type: 'date',
          format: 'date_optional_time||epoch_millis',
        },
        message: {
          type: 'wildcard',
        },
        service: {
          type: 'object',
          properties: {
            name: {
              type: 'keyword',
              time_series_dimension: true,
            },
          },
        },
      },
    };
  }

  getDimensions(): string[] {
    return ['service.name'];
  }

  getWriteTarget(document: Record<string, any>): string | null {
    if (!document.processor?.event) {
      throw Error("'processor.event' is not set on document, can not determine target index");
    }
    const eventType = document.processor.event;
    if (eventType === 'service') return 'metrics-apm.service-default';
    return null;
  }

  private state: Record<string, LatencyState> = {};
  private timestamp: number | null = null;

  process(event: ApmFields): Fields[] | null {
    if (event['processor.event'] !== 'transaction') return null;
    if (!event['@timestamp']) return null;
    if (!this.timestamp) this.timestamp = event['@timestamp'];

    const service = event['service.name']!;
    if (!this.state[service]) {
      this.state[service] = { count: 0, mean: 0 };
    }
    const duration = Number(event['transaction.duration.us']);
    const count = ++this.state[service].count;
    const differential = (duration - this.state[service].mean) / count;
    this.state[service].mean = this.state[service].mean + differential;
    if (Object.keys(this.state).length === 1000) {
      return this.createFieldsFromState();
    }
    const diff = Math.abs(event['@timestamp'] - this.timestamp);
    if (diff >= 1000 * 60) {
      return this.createFieldsFromState();
    }
    return null;
  }

  flush(): Fields[] {
    return this.createFieldsFromState();
  }

  private createFieldsFromState(): ServiceFields[] {
    const fields = Object.keys(this.state).map((service) => {
      return {
        '@timestamp': this.timestamp!,
        'metricset.name': 'service',
        'processor.event': 'service',
        'service.name': service,
        'service.latency_mean': this.state[service].mean,
        'service.latency_count': this.state[service].count,
      };
    });
    this.state = {};
    this.timestamp = null;
    return fields;
  }

  async bootstrapElasticsearch(esClient: Client): Promise<void> {}
}
