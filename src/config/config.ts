import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const configSchema = z.object({
  NODE_ENV: z.string().default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1),
  RABBITMQ_URL: z.string().min(1),
  APP_NAME: z.string().default('gateway-notificacao'),
  NOTIFICATION_PROVIDER_MODE: z.enum(['random-fail', 'always-fail']).default('random-fail'),
  RABBITMQ_MAIN_QUEUE: z.string().default('notifications.main'),
  WORKER_PREFETCH: z.coerce.number().int().positive().default(10),
  RABBITMQ_RETRY_1M_QUEUE: z.string().default('notifications.retry.1m'),
  RABBITMQ_RETRY_5M_QUEUE: z.string().default('notifications.retry.5m'),
  RABBITMQ_RETRY_15M_QUEUE: z.string().default('notifications.retry.15m'),
  RABBITMQ_DLQ_QUEUE: z.string().default('notifications.dlq'),
});

export const config = configSchema.parse(process.env);
