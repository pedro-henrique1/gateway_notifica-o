import { PrismaClient } from '@prisma/client';
import type { NotificationPayload, NotificationRecord, NotificationStatus } from '../types.js';

const globalForPrisma = globalThis as unknown as {
  prisma?: PrismaClient;
};

export const prisma = globalForPrisma.prisma ?? new PrismaClient();

if (process.env.NODE_ENV !== 'production') {
  globalForPrisma.prisma = prisma;
}

function mapNotification(notification: {
  id: string;
  recipient: string;
  type: string;
  status: string;
  payload: unknown;
  createdAt: Date;
  updatedAt: Date;
}): NotificationRecord {
  return {
    id: notification.id,
    recipient: notification.recipient,
    type: notification.type as NotificationRecord['type'],
    status: notification.status as NotificationStatus,
    payload: notification.payload as NotificationPayload,
    created_at: notification.createdAt,
    updated_at: notification.updatedAt,
  };
}

export async function initDatabase(): Promise<void> {
  await prisma.$connect();
}

export async function createNotification(notification: {
  id: string;
  recipient: string;
  type: string;
  payload: NotificationPayload;
}): Promise<NotificationRecord> {
  const createdNotification = await prisma.notification.create({
    data: {
      id: notification.id,
      recipient: notification.recipient,
      type: notification.type as NotificationRecord['type'],
      status: 'PENDING',
      payload: notification.payload,
    },
  });

  return mapNotification(createdNotification);
}

export async function getNotificationById(id: string): Promise<NotificationRecord | null> {
  const notification = await prisma.notification.findUnique({
    where: { id },
  });

  return notification ? mapNotification(notification) : null;
}

export async function updateNotificationStatus(id: string, status: NotificationStatus): Promise<void> {
  await prisma.notification.update({
    where: { id },
    data: {
      status,
    },
  });
}
