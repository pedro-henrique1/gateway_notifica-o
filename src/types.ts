import type { Prisma } from '@prisma/client';

export type NotificationType = 'EMAIL' | 'SMS' | 'PUSH';
export type NotificationStatus = 'PENDING' | 'SENT' | 'FAILED';

export interface NotificationPayload extends Prisma.JsonObject {
  recipient: string;
  type: NotificationType;
  content: string;
}

export interface NotificationRecord {
  id: string;
  recipient: string;
  type: NotificationType;
  status: NotificationStatus;
  payload: NotificationPayload;
  created_at: Date;
  updated_at: Date;
}
