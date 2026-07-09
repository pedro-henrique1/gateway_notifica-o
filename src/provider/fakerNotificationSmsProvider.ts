import type { NotificationRecord } from '../types.js';

export class FakeNotificationSmsProvider {
  async send(notification: NotificationRecord): Promise<void> {
    const shouldFail = Math.random() < 0.2;
    
    await new Promise((resolve) => setTimeout(resolve, 500));

    if (shouldFail) {
      throw new Error(`Failed to deliver SMS notification ${notification.id}`);
    }
  }
}