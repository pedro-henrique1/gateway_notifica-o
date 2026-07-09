import amqp from 'amqplib';
import { config } from '../config/config.js';
import { logger } from '../config/logger.js';

export interface NotificationJob {
  notificationId: string;
}

export interface NotificationEnvelope {
  job: NotificationJob;
  attempt: number;
}

type RabbitConnection = Awaited<ReturnType<typeof amqp.connect>>;
type RabbitChannel = Awaited<ReturnType<RabbitConnection['createChannel']>>;

let connection: RabbitConnection | null = null;
let channel: RabbitChannel | null = null;

const RETRY_POLICIES = [
  {
    attempt: 2,
    queue: config.RABBITMQ_RETRY_1M_QUEUE,
    ttl: 60_000,
  },
  {
    attempt: 3,
    queue: config.RABBITMQ_RETRY_5M_QUEUE,
    ttl: 300_000,
  },
  {
    attempt: 4,
    queue: config.RABBITMQ_RETRY_15M_QUEUE,
    ttl: 900_000,
  },
];

async function assertQueues(activeChannel: RabbitChannel): Promise<void> {
  await activeChannel.assertQueue(config.RABBITMQ_MAIN_QUEUE, {
    durable: true,
  });

  for (const policy of RETRY_POLICIES) {
    await activeChannel.assertQueue(policy.queue, {
      durable: true,
      arguments: {
        'x-message-ttl': policy.ttl,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': config.RABBITMQ_MAIN_QUEUE,
      },
    });
  }

  await activeChannel.assertQueue(config.RABBITMQ_DLQ_QUEUE, {
    durable: true,
  });
}

export async function connectRabbitMQ(): Promise<RabbitChannel> {
  if (channel) {
    return channel;
  }

  connection = await amqp.connect(config.RABBITMQ_URL);
  connection.on('close', () => {
    connection = null;
    channel = null;
  });
  connection.on('error', (error) => {
    logger.error({ err: error }, 'rabbitmq connection error');
  });

  channel = await connection.createChannel();
  await assertQueues(channel);

  return channel;
}

function createMessageContent(job: NotificationJob): Buffer {
  return Buffer.from(JSON.stringify(job));
}

export async function publishNotificationJob(job: NotificationJob, attempt = 1): Promise<void> {
  await publishToQueue(config.RABBITMQ_MAIN_QUEUE, job, attempt);
}

export async function publishToQueue(queueName: string, job: NotificationJob, attempt: number): Promise<void> {
  const activeChannel = await connectRabbitMQ();
  const content = createMessageContent(job);

  const isPublished = activeChannel.sendToQueue(queueName, content, {
    persistent: true,
    contentType: 'application/json',
    headers: {
      'x-retry-attempt': attempt,
    },
  });

  if (!isPublished) {
    throw new Error('Failed to publish notification job to RabbitMQ');
  }
}

export async function closeRabbitMQ(): Promise<void> {
  await channel?.close();
  await connection?.close();
  channel = null;
  connection = null;
}
