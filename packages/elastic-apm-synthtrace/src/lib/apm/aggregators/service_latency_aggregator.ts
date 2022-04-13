/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import { random } from 'lodash';
import { Client } from '@elastic/elasticsearch';
import { ApmFields, Observer } from '../apm_fields';
import { Fields } from '../../entity';
import { StreamAggregator } from '../../stream_aggregator';

interface LatencyState {
  count: number;
  mean: number;
  timestamp: number;
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
    'service.latency_mean': number;
    'service.latency_count': number;
  }>;

export class ServiceLatencyAggregator implements StreamAggregator<ApmFields> {
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
            latency_mean: {
              type: 'double',
              time_series_metric: 'gauge',
            },
            latency_count: {
              type: 'long',
              time_series_metric: 'gauge',
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

  private processedComponent: number = 0;

  process(event: ApmFields): Fields[] | null {
    if (event['processor.event'] !== 'transaction') return null;
    if (!event['@timestamp']) return null;

    const service = event['service.name']!;
    if (!this.state[service]) {
      this.state[service] = {
        count: 0,
        mean: 0,
        timestamp: event['@timestamp'],
      };
    }
    const duration = Number(event['transaction.duration.us']);
    const count = ++this.state[service].count;
    const differential = (duration - this.state[service].mean) / count;
    this.state[service].mean = this.state[service].mean + differential;

    if (Object.keys(this.state).length === 1000) {
      return this.createFieldsFromState();
    }

    const diff = Math.abs(event['@timestamp'] - this.state[service].timestamp);
    if (diff >= 1000 * 60) {
      const fields = this.createServiceFields(service);
      delete this.state[service];
      return [fields];
    }
    return null;
  }

  flush(): Fields[] {
    return this.createFieldsFromState();
  }

  private createFieldsFromState(): ServiceFields[] {
    const fields = Object.keys(this.state).map((service) => this.createServiceFields(service));
    this.state = {};
    return fields;
  }

  private createServiceFields(service: string): ServiceFields {
    this.processedComponent = ++this.processedComponent % 1000;
    const component = Date.now() % 100;
    return {
      '@timestamp':
        this.state[service].timestamp + random(0, 100) + component + this.processedComponent,
      'metricset.name': 'service',
      'processor.event': 'service',
      'service.name': service,
      'service.latency_mean': this.state[service].mean,
      'service.latency_count': this.state[service].count,
    };
  }

  async bootstrapElasticsearch(esClient: Client): Promise<void> {}
}
