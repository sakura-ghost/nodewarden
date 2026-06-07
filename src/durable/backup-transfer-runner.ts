import type { Env } from '../types';
import type { BackupDestinationRecord } from '../services/backup-config';
import {
  BACKUP_SCHEDULER_WINDOW_MINUTES,
  hasBackupSlotBetween,
  isBackupDueNow,
  loadBackupSettings,
} from '../services/backup-config';
import { createRemoteBackupTransferSession } from '../services/backup-uploader';
import { getBlobObject } from '../services/blob-store';
import { StorageService } from '../services/storage';
import { notifyUserBackupProgress } from './notifications-hub';
import { executeConfiguredBackup } from '../handlers/backup';

const BACKUP_JOB_STATE_KEY = 'backup.job.state.v1';
const BACKUP_JOB_LEASE_MS = 10 * 60 * 1000;
const BACKUP_JOB_HEARTBEAT_MS = 30 * 1000;

interface BackupJobState {
  token: string;
  reason: string;
  acquiredAt: string;
  touchedAt: string;
  expiresAtMs: number;
}

interface RemoteAttachmentChunkRequest {
  destination: BackupDestinationRecord;
  attachments: Array<{
    blobName: string;
  }>;
}

interface ConfiguredBackupRunRequest {
  actorUserId?: string | null;
  auditMetadata?: Record<string, unknown> | null;
  destinationId?: string | null;
  targetDeviceIdentifier?: string | null;
  trigger?: 'manual' | 'scheduled';
}

function badRequest(message: string, status: number = 400): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
    },
  });
}

export class BackupTransferRunner {
  private lastHeartbeatAt = 0;

  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env
  ) {
  }

  private async acquireJob(reason: string): Promise<string | null> {
    const nowMs = Date.now();
    const current = await this.state.storage.get<BackupJobState>(BACKUP_JOB_STATE_KEY);
    if (current?.expiresAtMs && current.expiresAtMs > nowMs) {
      return null;
    }

    const token = crypto.randomUUID();
    const nowIso = new Date(nowMs).toISOString();
    await this.state.storage.put<BackupJobState>(BACKUP_JOB_STATE_KEY, {
      token,
      reason,
      acquiredAt: nowIso,
      touchedAt: nowIso,
      expiresAtMs: nowMs + BACKUP_JOB_LEASE_MS,
    });
    this.lastHeartbeatAt = 0;
    return token;
  }

  private async touchJob(token: string): Promise<void> {
    const nowMs = Date.now();
    if (nowMs - this.lastHeartbeatAt < BACKUP_JOB_HEARTBEAT_MS) return;
    this.lastHeartbeatAt = nowMs;

    const current = await this.state.storage.get<BackupJobState>(BACKUP_JOB_STATE_KEY);
    if (current?.token !== token) return;

    await this.state.storage.put<BackupJobState>(BACKUP_JOB_STATE_KEY, {
      ...current,
      touchedAt: new Date(nowMs).toISOString(),
      expiresAtMs: nowMs + BACKUP_JOB_LEASE_MS,
    });
  }

  private async releaseJob(token: string): Promise<void> {
    const current = await this.state.storage.get<BackupJobState>(BACKUP_JOB_STATE_KEY);
    if (current?.token === token) {
      await this.state.storage.delete(BACKUP_JOB_STATE_KEY);
    }
  }

  private async runConfiguredBackup(request: Request): Promise<Response> {
    let body: ConfiguredBackupRunRequest;
    try {
      body = await request.json<ConfiguredBackupRunRequest>();
    } catch {
      return badRequest('Backup run payload is invalid');
    }

    const trigger = body.trigger === 'scheduled' ? 'scheduled' : 'manual';
    const actorUserId = String(body.actorUserId || '').trim() || null;
    if (trigger === 'manual' && !actorUserId) {
      return badRequest('Manual backup run requires an actor');
    }

    const token = await this.acquireJob(`${trigger}:${actorUserId || 'system'}`);
    if (!token) {
      return badRequest('Another backup run is already in progress', 409);
    }

    try {
      await this.touchJob(token);
      const storage = new StorageService(this.env.DB);
      const progress = actorUserId
        ? async (event: {
          operation: 'backup-remote-run';
          step: string;
          fileName: string;
          stageTitle: string;
          stageDetail: string;
          done?: boolean;
          ok?: boolean;
          error?: string | null;
        }) => {
          await notifyUserBackupProgress(
            this.env,
            actorUserId,
            event,
            String(body.targetDeviceIdentifier || '').trim() || null
          );
        }
        : null;

      const result = await executeConfiguredBackup(
        this.env,
        storage,
        actorUserId,
        trigger,
        body.destinationId || null,
        () => this.touchJob(token),
        progress,
        body.auditMetadata || null
      );
      const settings = await loadBackupSettings(storage, this.env, 'UTC');

      return new Response(JSON.stringify({
        object: 'backup-runner-result',
        result,
        settings,
      }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store',
        },
      });
    } catch (error) {
      return badRequest(error instanceof Error ? error.message : 'Backup run failed', 500);
    } finally {
      await this.releaseJob(token);
    }
  }

  private async runScheduledBackups(): Promise<Response> {
    const token = await this.acquireJob('scheduled');
    if (!token) {
      return badRequest('Another backup run is already in progress', 409);
    }

    let completed = 0;
    try {
      await this.touchJob(token);
      const storage = new StorageService(this.env.DB);
      let scanStartMs = Date.now();

      while (true) {
        await this.touchJob(token);
        const settings = await loadBackupSettings(storage, this.env, 'UTC');
        const now = new Date();
        const dueDestinations = settings.destinations.filter((destination) =>
          isBackupDueNow(destination, now, BACKUP_SCHEDULER_WINDOW_MINUTES)
          || hasBackupSlotBetween(destination, new Date(scanStartMs), now)
        );

        if (!dueDestinations.length) {
          break;
        }

        scanStartMs = now.getTime();
        for (const destination of dueDestinations) {
          await this.touchJob(token);
          await executeConfiguredBackup(
            this.env,
            storage,
            null,
            'scheduled',
            destination.id,
            () => this.touchJob(token)
          );
          completed += 1;
        }
      }

      return new Response(JSON.stringify({
        ok: true,
        completed,
      }), {
        status: 200,
        headers: {
          'Content-Type': 'application/json; charset=utf-8',
          'Cache-Control': 'no-store',
        },
      });
    } catch (error) {
      return badRequest(error instanceof Error ? error.message : 'Scheduled backup failed', 500);
    } finally {
      await this.releaseJob(token);
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== 'POST') {
      return badRequest('Not found', 404);
    }

    if (url.pathname === '/internal/run-configured-backup') {
      return this.runConfiguredBackup(request);
    }

    if (url.pathname === '/internal/run-scheduled-backups') {
      return this.runScheduledBackups();
    }

    if (url.pathname !== '/internal/upload-attachment-chunk') {
      return badRequest('Not found', 404);
    }

    let body: RemoteAttachmentChunkRequest;
    try {
      body = await request.json<RemoteAttachmentChunkRequest>();
    } catch {
      return badRequest('Attachment chunk payload is invalid');
    }

    if (!body?.destination || !Array.isArray(body.attachments)) {
      return badRequest('Attachment chunk payload is invalid');
    }

    const remoteSession = createRemoteBackupTransferSession(body.destination);
    let uploaded = 0;

    for (const attachment of body.attachments) {
      const blobName = String(attachment?.blobName || '').trim();
      if (!blobName) {
        return badRequest('Attachment chunk payload is invalid');
      }

      const object = await getBlobObject(this.env, blobName);
      if (!object) {
        return badRequest(`Attachment blob missing for ${blobName}`, 409);
      }

      const bytes = new Uint8Array(await new Response(object.body).arrayBuffer());
      await remoteSession.putFile(`attachments/${blobName}`, bytes, {
        contentType: object.contentType,
      });
      uploaded += 1;
    }

    return new Response(JSON.stringify({
      ok: true,
      uploaded,
    }), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  }
}
