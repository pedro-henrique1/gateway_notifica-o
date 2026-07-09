import type { NotificationRecord } from '../types.js';

export class FailingNotificationEmailProvider {
  async send(notification: NotificationRecord): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 500));
    throw new Error(`Forced failure for notification ${notification.id}`);
  }
}
