import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from '@aws-sdk/client-secrets-manager';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Client } from 'pg';

const secretsClient = new SecretsManagerClient({});
const s3Client = new S3Client({});

interface DbSecret {
  host: string;
  port: number;
  username: string;
  password: string;
  dbname: string;
}

interface RestoreEvent {
  key: string;
}

interface RestoreResult {
  status: string;
  key: string;
  message: string;
}

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

const downloadSql = async (key: string): Promise<string> => {
  const bucket = process.env.RESTORE_BUCKET;
  if (!bucket) {
    throw new Error('RESTORE_BUCKET is required');
  }

  const result = await s3Client.send(
    new GetObjectCommand({ Bucket: bucket, Key: key })
  );

  if (!result.Body) {
    throw new Error(`S3 object is empty: ${key}`);
  }

  return result.Body.transformToString('utf-8');
};

const validateKey = (key: string): void => {
  if (!key || typeof key !== 'string') {
    throw new Error('key is required');
  }

  // パストラバーサル防止
  if (key.includes('..') || key.includes('/') || key.includes('\\')) {
    throw new Error('Invalid key: path traversal is not allowed');
  }

  // .sql 拡張子のみ許可
  if (!key.endsWith('.sql')) {
    throw new Error('Invalid key: only .sql files are allowed');
  }
};

const sanitizeSql = (raw: string): string => {
  return raw
    .split('\n')
    .filter(
      (line) =>
        !line.startsWith('\\') &&
        !line.includes('ag_catalog') &&
        !line.includes('DISABLE TRIGGER') &&
        !line.includes('ENABLE TRIGGER') &&
        !line.includes('set_config(\'search_path\'')
    )
    .join('\n');
};

export const handler = async (event: RestoreEvent): Promise<RestoreResult> => {
  validateKey(event.key);

  console.log(`Restoring from S3 key: ${event.key}`);

  const [rawSql, credentials] = await Promise.all([
    downloadSql(event.key),
    getDbCredentials(),
  ]);

  const sql = sanitizeSql(rawSql);
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

    // リストア前に全テーブルを TRUNCATE
    const tables = await client.query(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public'
        AND tablename != '_migration_history'
    `);
    for (const row of tables.rows) {
      await client.query(`TRUNCATE TABLE public."${row.tablename}" CASCADE`);
    }
    console.log(`Truncated ${tables.rows.length} tables`);

    // FK 制約を一時的に無効化してリストア
    // (banks.owner_entity_id → entities.id の循環参照対策)
    const fkConstraints = await client.query(`
      SELECT conname, conrelid::regclass AS tablename,
             pg_get_constraintdef(oid) AS definition
      FROM pg_constraint
      WHERE contype = 'f'
        AND connamespace = 'public'::regnamespace
    `);
    for (const fk of fkConstraints.rows) {
      await client.query(`ALTER TABLE ${fk.tablename} DROP CONSTRAINT "${fk.conname}"`);
    }
    console.log(`Dropped ${fkConstraints.rows.length} FK constraints`);

    await client.query(sql);

    // search_path を復元（pg_dump が空に設定するため）
    await client.query(`SET search_path = public`);

    // FK 制約を復元
    for (const fk of fkConstraints.rows) {
      await client.query(`ALTER TABLE ${fk.tablename} ADD CONSTRAINT "${fk.conname}" ${fk.definition}`);
    }
    console.log(`Restored ${fkConstraints.rows.length} FK constraints`);

    console.log(`Restore completed: ${event.key}`);

    return {
      status: 'success',
      key: event.key,
      message: 'Restore completed successfully',
    };
  } catch (error) {
    console.error('Restore failed:', error);
    throw new Error('Restore operation failed');
  } finally {
    await client.end().catch(() => {});
  }
};
