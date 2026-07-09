import { randomUUID } from 'node:crypto';
import express from 'express';
import { z } from 'zod';
import { config } from './config/config.js';
import { createNotification, initDatabase, updateNotificationStatus } from './database/prisma.js';
import { logger } from './config/logger.js';
import { publishNotificationJob } from './queue/rabbitmq.js';

const notificationRequestSchema = z.object({
  recipient: z.string().min(1),
  type: z.enum(['EMAIL', 'SMS', 'PUSH']),
  content: z.string().min(1),
});

const app = express();
app.use(express.json());
app.use((request, response, next) => {
  const requestId = typeof request.headers['x-request-id'] === 'string' && request.headers['x-request-id'].length > 0
    ? request.headers['x-request-id']
    : randomUUID();

  response.setHeader('x-request-id', requestId);
  response.locals.requestId = requestId;

  next();
});

app.get('/health', (_request, response) => {
  response.status(200).json({ status: 'ok' });
});

app.post('/api/v1/notifications', async (request, response, next) => {
  try {
    const requestId = response.locals.requestId as string;
    const requestLogger = logger.child({ requestId });
    const payload = notificationRequestSchema.parse(request.body);
    const id = randomUUID();

    const notification = await createNotification({
      id,
      recipient: payload.recipient,
      type: payload.type,
      payload,
    });

    try {
      await publishNotificationJob({ notificationId: notification.id });
    } catch (publishError) {
      await updateNotificationStatus(notification.id, 'FAILED');
      throw publishError;
    }

    requestLogger.info({ notificationId: notification.id }, 'notification accepted');

    response.status(202).json({
      message: 'Notification accepted',
      notification,
    });
  } catch (error) {
    next(error);
  }
});

app.use((error: unknown, _request: express.Request, response: express.Response, _next: express.NextFunction) => {
  const requestId = response.locals.requestId as string | undefined;
  const requestLogger = requestId ? logger.child({ requestId }) : logger;

  if (error instanceof z.ZodError) {
    requestLogger.warn({ issues: error.issues }, 'invalid notification request');

    response.status(400).json({
      message: 'Invalid request body',
      issues: error.issues,
    });
    return;
  }

  requestLogger.error({ err: error }, 'unexpected api error');
  response.status(500).json({
    message: 'Internal server error',
  });
});

async function bootstrap(): Promise<void> {
  await initDatabase();

  app.listen(config.PORT, () => {
    logger.info({ port: config.PORT }, 'api listening');
  });
}

void bootstrap();
