import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const configSchema = z.object({
  NODE_ENV: z.string().default('development'),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1),
  RABBITMQ_URL: z.string().min(1),
  APP_NAME: z.string().default('gateway-notificacao'),
  RABBITMQ_MAIN_QUEUE: z.string().default('notifications.main'),
  WORKER_PREFETCH: z.coerce.number().int().positive().default(10),
});

export const config = configSchema.parse(process.env);
