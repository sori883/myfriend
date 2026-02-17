import { z } from 'zod';

/**
 * .envに応じたバリデーションスキーマを定義する
 */
const dotEnvSchema = z.object({
  ACCOUNT_ID: z.string(),
});

const validatedDotEnv = dotEnvSchema.safeParse(process.env);

if (!validatedDotEnv.success) {
  console.error('❌ Invalid .env variables');
  throw new Error('Invalid .env variables');
}

export const env = validatedDotEnv.data;
