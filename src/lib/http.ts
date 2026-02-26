export interface FetchJsonRetryOptions {
  attempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  timeoutMs?: number;
}

class FetchJsonHttpError extends Error {
  readonly status: number;
  readonly retryable: boolean;

  constructor(message: string, status: number, retryable: boolean) {
    super(message);
    this.name = "FetchJsonHttpError";
    this.status = status;
    this.retryable = retryable;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function shouldRetryStatus(status: number): boolean {
  return status === 408 || status === 429 || status >= 500;
}

export async function fetchJsonWithRetry<T>(
  url: string,
  init: RequestInit = {},
  options: FetchJsonRetryOptions = {},
): Promise<T> {
  const attempts = options.attempts ?? 4;
  const initialDelayMs = options.initialDelayMs ?? 500;
  const maxDelayMs = options.maxDelayMs ?? 5_000;
  const timeoutMs = options.timeoutMs ?? 20_000;

  let delayMs = initialDelayMs;
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...init,
        signal: controller.signal,
      });
      if (!response.ok) {
        const responseText = await response.text();
        const retryable = shouldRetryStatus(response.status);
        if (attempt < attempts && retryable) {
          await sleep(delayMs);
          delayMs = Math.min(delayMs * 2, maxDelayMs);
          continue;
        }
        throw new FetchJsonHttpError(
          `Request failed (${response.status}) for ${url}: ${responseText.slice(0, 300)}`,
          response.status,
          retryable,
        );
      }
      return (await response.json()) as T;
    } catch (error) {
      lastError = error;
      if (error instanceof FetchJsonHttpError && !error.retryable) {
        throw error;
      }
      if (attempt >= attempts) {
        break;
      }
      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, maxDelayMs);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  throw new Error(`Failed request after ${attempts} attempts for ${url}: ${String(lastError)}`);
}
