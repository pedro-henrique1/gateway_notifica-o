import { randomUUID } from 'node:crypto';
import express from 'express';
import { z } from 'zod';
import { config } from './config.js';
import { createNotification, initDatabase, updateNotificationStatus } from './database/prisma.js';
import { publishNotificationJob } from './queue/rabbitmq.js';

const notificationRequestSchema = z.object({
  recipient: z.string().min(1),
  type: z.enum(['EMAIL', 'SMS', 'PUSH']),
  content: z.string().min(1),
});

const app = express();
app.use(express.json());

app.get('/health', (_request, response) => {
  response.status(200).json({ status: 'ok' });
});

app.post('/api/v1/notifications', async (request, response, next) => {
  try {
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

    response.status(202).json({
      message: 'Notification accepted',
      notification,
    });
  } catch (error) {
    next(error);
  }
});

app.use((error: unknown, _request: express.Request, response: express.Response, _next: express.NextFunction) => {
  if (error instanceof z.ZodError) {
    response.status(400).json({
      message: 'Invalid request body',
      issues: error.issues,
    });
    return;
  }

  console.error(error);
  response.status(500).json({
    message: 'Internal server error',
  });
});

async function bootstrap(): Promise<void> {
  await initDatabase();

  app.listen(config.PORT, () => {
    console.log(`API listening on port ${config.PORT}`);
  });
}

void bootstrap();
