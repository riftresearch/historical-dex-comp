const SECONDS_PER_DAY = 24 * 60 * 60;

export function addDays(date: Date, days: number): Date {
  return new Date(date.getTime() + days * SECONDS_PER_DAY * 1000);
}

export function shiftSeconds(date: Date, seconds: number): Date {
  return new Date(date.getTime() + seconds * 1000);
}

export function toUnixSeconds(date: Date): number {
  return Math.floor(date.getTime() / 1000);
}

export function fromUnixSeconds(seconds: number): Date {
  return new Date(seconds * 1000);
}

export function toIsoOrNull(date: Date | null | undefined): string | null {
  if (!date) {
    return null;
  }

  return date.toISOString();
}

export function parseDateOrNull(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (typeof value === "number") {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  if (typeof value === "string") {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  if (typeof value === "object") {
    const toString = value.toString;
    if (typeof toString === "function") {
      const text = toString.call(value);
      if (text && text !== "[object Object]") {
        const parsed = new Date(text);
        if (!Number.isNaN(parsed.getTime())) {
          return parsed;
        }
      }
    }
  }

  return null;
}

export function parseUnixSecondsOrNull(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsedNumber = Number(value);
  if (!Number.isFinite(parsedNumber) || parsedNumber <= 0) {
    return null;
  }

  return fromUnixSeconds(parsedNumber);
}
