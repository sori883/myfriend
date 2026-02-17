import { z } from 'zod';

const envNameSchema = z.enum(['prd', 'stg', 'dev']);

/**
 * 環境の型
 */
export type EnvNameType = z.infer<typeof envNameSchema>;

/**
 * 環境の型ガード
 * @param value 環境名（例：app.node.tryGetContext('env')など）
 * @returns 環境名
 */
export function validateEnvName(envName: unknown): EnvNameType {
  const validateEnvName = envNameSchema.safeParse(envName);

  if (!validateEnvName.success) {
    console.error('❌ Invalid environment name');
    throw new Error('Invalid environment name');
  }

  return validateEnvName.data;
}
