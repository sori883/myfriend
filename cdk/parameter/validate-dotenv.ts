import { z } from 'zod';

/**
 * .envに応じたバリデーションスキーマを定義する
 */
const dotEnvSchema = z.object({
  ACCOUNT_ID: z.string(),
  AGENT_MODEL_ID: z.string(),
  EXTRACTION_MODEL_ID: z.string(),
  EMBEDDING_MODEL_ID: z.string(),
  RERANK_MODEL_ID: z.string(),
  REFLECT_MODEL_ID: z.string(),
  PREFERENCE_MODEL_ID: z.string(),
  TAVILY_API_KEY: z.string().default(''),
});

const validatedDotEnv = dotEnvSchema.safeParse(process.env);

if (!validatedDotEnv.success) {
  console.error('❌ Invalid .env variables');
  throw new Error('Invalid .env variables');
}

export const env = validatedDotEnv.data;
