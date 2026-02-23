import { createHash } from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from '@aws-sdk/client-secrets-manager';
import { Client } from 'pg';

const secretsClient = new SecretsManagerClient({});

interface DbSecret {
  host: string;
  port: number;
  username: string;
  password: string;
  dbname: string;
}

interface CdkEvent {
  RequestType: 'Create' | 'Update' | 'Delete';
  ResourceProperties: Record<string, string>;
}

/**
 * DB認証情報を Secrets Manager から取得
 */
const getDbCredentials = async (): Promise<DbSecret> => {
  const secretArn = process.env.DB_SECRET_ARN;
  if (!secretArn) {
    throw new Error('DB_SECRET_ARN is required');
  }

  const result = await secretsClient.send(
    new GetSecretValueCommand({ SecretId: secretArn })
  );

  if (!result.SecretString) {
    throw new Error('Secret string is empty');
  }

  return JSON.parse(result.SecretString);
};

/**
 * マイグレーション履歴テーブルを作成（べき等）
 */
const ensureMigrationTable = async (client: Client): Promise<void> => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS _migration_history (
      id SERIAL PRIMARY KEY,
      filename TEXT NOT NULL UNIQUE,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      checksum TEXT
    )
  `);
};

/**
 * SQL ファイルを順番に実行
 */
const runMigrations = async (client: Client): Promise<string[]> => {
  const sqlDir = path.join(__dirname, 'sql');
  const files = fs.readdirSync(sqlDir).filter(f => f.endsWith('.sql')).sort();
  const applied: string[] = [];

  for (const file of files) {
    const sql = fs.readFileSync(path.join(sqlDir, file), 'utf-8');
    const checksum = createHash('sha256').update(sql).digest('hex');

    // 既に適用済みか確認
    const { rows } = await client.query(
      'SELECT checksum FROM _migration_history WHERE filename = $1',
      [file]
    );

    if (rows.length > 0) {
      if (rows[0].checksum && rows[0].checksum !== checksum) {
        throw new Error(
          `Migration ${file} has been modified after being applied`
        );
      }
      continue;
    }

    // トランザクション内で SQL 実行 + 履歴記録
    await client.query('BEGIN');
    try {
      await client.query(sql);
      await client.query(
        'INSERT INTO _migration_history (filename, checksum) VALUES ($1, $2)',
        [file, checksum]
      );
      await client.query('COMMIT');
      applied.push(file);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  }

  return applied;
};

export const handler = async (event: CdkEvent): Promise<{ Data: Record<string, string> }> => {
  // Delete イベントでは何もしない
  if (event.RequestType === 'Delete') {
    return { Data: { status: 'skipped' } };
  }

  const credentials = await getDbCredentials();
  const dbName = process.env.DB_NAME || credentials.dbname;

  const client = new Client({
    host: credentials.host,
    port: credentials.port || 5432,
    user: credentials.username,
    password: credentials.password,
    database: dbName,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();
    await ensureMigrationTable(client);
    const applied = await runMigrations(client);

    return {
      Data: {
        status: 'success',
        applied: applied.join(', ') || 'none (already up to date)',
      },
    };
  } finally {
    await client.end();
  }
};
